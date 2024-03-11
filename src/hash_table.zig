const std = @import("std");
const assert = std.debug.assert;
const hashStringV1 = @import("hash.zig").hashStringV1;
const log = std.log;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;

/// Represents the PDB serializable hash table.
/// This container is used internally in the PDB file format.
/// The value type is generic, while the key type is fixed and equal to `u32`.
///
/// Insertion/lookup in presence of hash collisions:
/// (based on LLVM's official implementation)
///
///     Insertion is done via the so-called linear probing, that is,
///     for any given hash value of the key, we get the start index into
///     the bucket in the hash table. If the bucket at that index is already
///     occupied, we iterate the subsequent buckets until we hit an empty/deleted one.
///
/// Load factor:
///
///     According to Microsoft's official requirements, the load factor should never
///     exceed `capacity * 2 / 3 + 1` or approx 67%.
///
/// For more info, see:
/// https://llvm.org/docs/PDB/HashTable.html
pub fn HashTable(comptime Value: type, comptime Context: type) type {
    return struct {
        header: Header = .{ .size = 0, .capacity = 0 },
        buckets: std.ArrayListUnmanaged(Entry) = .{},
        present: DynamicBitSetUnmanaged = .{},
        deleted: DynamicBitSetUnmanaged = .{},

        const Self = @This();

        const Header = extern struct {
            size: u32,
            capacity: u32,
        };

        pub const Key = u32;

        pub const Entry = extern struct {
            key: Key,
            value: Value,
        };

        pub fn ensureTotalCapacity(self: *Self, allocator: Allocator, new_size: u32, ctx: Context) !void {
            if (new_size > self.count()) {
                try self.grow(allocator, capacityForSize(new_size), ctx);
            }
        }

        pub fn ensureUnusedCapacity(self: *Self, allocator: Allocator, additional_size: u32, ctx: Context) !void {
            return self.ensureTotalCapacity(allocator, self.count() + additional_size, ctx);
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.deallocate(allocator);
            self.* = undefined;
        }

        /// Returns the number of values present in the hash table.
        pub fn count(self: Self) u32 {
            return self.header.size;
        }

        /// Returns current capacity (a sum of slots taken and available).
        pub fn capacity(self: Self) u32 {
            return self.header.capacity;
        }

        /// Returns the corresponding value for key if one exists.
        /// Otherwise, returns null.
        pub fn get(self: Self, key: anytype, ctx: anytype) ?Value {
            const index = self.getIndex(key, ctx) orelse return null;
            return self.buckets.items[index].value;
        }

        const GetOrPutResult = struct {
            found_existing: bool,
            key_ptr: *Key,
            value_ptr: *Value,
        };

        /// If the key exists, returns pointers to that bucket.
        /// Otherwise, finds the first empty bucket and returns a pointer to it.
        /// If the key did not exist, it will *not* be set in the bucket's key_ptr -
        /// the caller needs to this once this function returns.
        /// Our implementation differs from that in LLVM in that we do not insert
        /// a new element until we ensure we have enough of capacity available.
        pub fn getOrPut(
            self: *Self,
            allocator: Allocator,
            key: anytype,
            key_ctx: anytype,
            ctx: Context,
        ) !GetOrPutResult {
            if (self.getIndex(key, key_ctx)) |index| {
                assert(self.present.isSet(index));
                assert(key_ctx.eql(key, self.buckets.items[index].key));
                return GetOrPutResult{
                    .found_existing = true,
                    .key_ptr = &self.buckets.items[index].key,
                    .value_ptr = &self.buckets.items[index].value,
                };
            }

            if (self.count() + 1 >= maxLoad(self.capacity())) {
                try self.grow(allocator, capacityForSize(self.count() + 1), ctx);
            }

            return self.getOrPutAssumeCapacity(key, key_ctx);
        }

        pub fn getOrPutAssumeCapacity(self: *Self, key: anytype, ctx: anytype) GetOrPutResult {
            const hash_bucket = ctx.hash(key) % self.capacity();
            var index = hash_bucket;
            var first_unused: ?u32 = null;

            while (true) {
                if (self.present.isSet(index)) {
                    if (ctx.eql(key, self.buckets.items[index].key)) {
                        return .{
                            .found_existing = true,
                            .key_ptr = &self.buckets.items[index].key,
                            .value_ptr = &self.buckets.items[index].value,
                        };
                    }
                } else {
                    if (first_unused == null) {
                        first_unused = index;
                    }

                    if (!self.deleted.isSet(index)) {
                        break;
                    }
                }

                index = (index + 1) % self.capacity();
                if (index == hash_bucket) break;
            }

            assert(first_unused != null); // Run out of capacity!
            const new_index = first_unused.?;
            self.present.set(new_index);
            self.deleted.unset(new_index);
            self.header.size += 1;

            return .{
                .found_existing = false,
                .key_ptr = &self.buckets.items[new_index].key,
                .value_ptr = &self.buckets.items[new_index].value,
            };
        }

        /// Calculates required number of bytes to serialize the HashTable
        /// to a byte stream.
        /// Use it to preallocate the output buffer.
        pub fn serializedSize(self: Self) u32 {
            var size: usize = @sizeOf(Header);

            for (&[_]DynamicBitSetUnmanaged{ self.present, self.deleted }) |vec| {
                if (findLastSet(vec)) |index| {
                    size += numMasks(u32, index + 1) * @sizeOf(u32);
                }
                size += @sizeOf(u32);
            }

            size += self.count() * @sizeOf(Entry);

            return @intCast(size);
        }

        /// Reads the HashTable from an input stream.
        /// HashTable is serialized according to the PDB spec found at:
        /// https://llvm.org/docs/PDB/HashTable.html
        pub fn read(allocator: Allocator, reader: anytype) !Self {
            const header = try reader.readStruct(Header);

            var self = Self{ .header = header };
            try self.allocate(allocator, header.capacity);
            self.header.size = header.size;

            var present = try readBitSet(u32, allocator, reader);
            defer present.deinit(allocator);

            if (present.count() != self.count()) {
                log.err("Present bit vector does not the match size of hash table", .{});
                return error.InvalidHashMap;
            }

            var deleted = try readBitSet(u32, allocator, reader);
            defer deleted.deinit(allocator);

            if (intersects(present, deleted)) {
                log.err("Buckets marked as both valid and deleted", .{});
                return error.InvalidHashMap;
            }

            {
                var i: usize = 0;
                while (i < numMasks(usize, present.bit_length)) : (i += 1) {
                    self.present.masks[i] = present.masks[i];
                }
            }

            {
                var i: usize = 0;
                while (i < numMasks(usize, deleted.bit_length)) : (i += 1) {
                    self.deleted.masks[i] = deleted.masks[i];
                }
            }

            var bucket_it = self.present.iterator(.{});
            while (bucket_it.next()) |index| {
                self.buckets.items[index] = try reader.readStruct(Entry);
            }

            return self;
        }

        /// Writes the HashTable to an output stream.
        /// HashTable is serialized according to the PDB spec found at:
        /// https://llvm.org/docs/PDB/HashTable.html
        pub fn write(self: Self, writer: anytype) !void {
            try writer.writeAll(@as([*]const u8, @ptrCast(&self.header))[0..@sizeOf(Header)]);

            for (&[_]DynamicBitSetUnmanaged{ self.present, self.deleted }) |vec| {
                // Calculate number of words for the bit vector
                const present_num_words: u32 = if (findLastSet(vec)) |index|
                    @intCast(numMasks(u32, index + 1))
                else
                    0;
                try writer.writeInt(u32, present_num_words, .little);

                // Serialize the sequence of bitvector's masks
                if (present_num_words > 0) {
                    const present_words: []const u32 = @as([*]const u32, @ptrCast(vec.masks))[0..present_num_words];
                    for (present_words) |word| {
                        try writer.writeInt(u32, word, .little);
                    }
                }
            }

            // Finally, serialize valid (present) buckets in the bucket list
            var it = self.present.iterator(.{});
            while (it.next()) |index| {
                const entry = self.buckets.items[index];
                try writer.writeInt(u32, entry.key, .little);
                try writer.writeAll(@as([*]const u8, @ptrCast(&entry.value))[0..@sizeOf(Value)]);
            }
        }

        pub const Iterator = struct {
            table: *const Self,
            iter: DynamicBitSetUnmanaged.Iterator(.{}),

            pub fn next(it: *Iterator) ?Entry {
                const index = it.iter.next() orelse return null;
                return it.table.buckets.items[index];
            }
        };

        pub fn iterator(self: *const Self) Iterator {
            return .{ .table = self, .iter = self.present.iterator(.{}) };
        }

        /// If the key exists, returns index to the bucket holding the entry.
        /// Otherwise returns null.
        fn getIndex(self: Self, key: anytype, ctx: anytype) ?u32 {
            if (self.count() == 0) return null;

            const hash_bucket = ctx.hash(key) % self.capacity();
            var index = hash_bucket;

            while (true) {
                if (self.present.isSet(index)) {
                    if (ctx.eql(key, self.buckets.items[index].key)) {
                        return index;
                    }
                } else {
                    if (!self.deleted.isSet(index)) {
                        break;
                    }
                }

                index = (index + 1) % self.capacity();
                if (index == hash_bucket) break;
            }

            return null;
        }

        fn grow(self: *Self, allocator: Allocator, new_capacity: u32, ctx: Context) !void {
            var table = Self{};
            defer table.deinit(allocator);
            try table.allocate(allocator, new_capacity);
            assert(table.capacity() == new_capacity);

            var present = self.present.iterator(.{});
            while (present.next()) |index| {
                const entry = self.buckets.items[index];
                const gop = table.getOrPutAssumeCapacity(entry.key, ctx);
                assert(!gop.found_existing);
                gop.key_ptr.* = entry.key;
                gop.value_ptr.* = entry.value;
            }

            mem.swap(Self, self, &table);
        }

        fn allocate(self: *Self, allocator: Allocator, new_capacity: u32) !void {
            self.header = .{
                .size = 0,
                .capacity = new_capacity,
            };
            try self.buckets.resize(allocator, self.capacity());
            self.present = try DynamicBitSetUnmanaged.initEmpty(allocator, self.capacity());
            self.deleted = try DynamicBitSetUnmanaged.initEmpty(allocator, self.capacity());
        }

        fn deallocate(self: *Self, allocator: Allocator) void {
            self.buckets.deinit(allocator);
            self.present.deinit(allocator);
            self.deleted.deinit(allocator);
        }

        inline fn maxLoad(cap: u32) u32 {
            return cap * 2 / 3 + 1;
        }

        fn capacityForSize(size: u32) u32 {
            const new_cap = @as(u64, size) * 3 / 2 + 1;
            return math.cast(u32, new_cap) orelse unreachable;
        }

        inline fn numMasks(comptime Word: type, bit_length: usize) usize {
            return (bit_length + (@bitSizeOf(Word) - 1)) / @bitSizeOf(Word);
        }
    };
}

fn intersects(lhs: DynamicBitSetUnmanaged, rhs: DynamicBitSetUnmanaged) bool {
    var lhs_it = lhs.iterator(.{});
    var rhs_it = rhs.iterator(.{});

    while (lhs_it.next()) |lhs_index| {
        while (rhs_it.next()) |rhs_index| {
            if (lhs_index == rhs_index) return true;
            if (lhs_index > rhs_index) break;
        } else return false;
    }

    return false;
}

fn readBitSet(comptime Word: type, allocator: Allocator, reader: anytype) !DynamicBitSetUnmanaged {
    const num_words = try reader.readInt(Word, .little);
    const bit_length = num_words * @bitSizeOf(Word);
    var bitset = try DynamicBitSetUnmanaged.initEmpty(allocator, bit_length);

    var i: usize = 0;
    while (i < num_words) : (i += 1) {
        const word = try reader.readInt(Word, .little);
        var index: std.math.Log2Int(Word) = 0;
        while (index < @bitSizeOf(Word) - 1) : (index += 1) {
            if ((word & (@as(Word, 1) << index)) != 0) {
                bitset.set(index);
            }
        }
    }

    return bitset;
}

fn findLastSet(bitset: DynamicBitSetUnmanaged) ?usize {
    var it = bitset.iterator(.{ .direction = .reverse });
    return it.next();
}

test "roundtrip test - compatibility with LLVM" {
    // Since our HashTable implementation is based on that of LLVM's,
    // input `buffer` is also the expected output buffer.
    const buffer: []const u8 =
        "\x02\x00\x00\x00\x04\x00\x00\x00" ++
        "\x01\x00\x00\x00\x06\x00\x00\x00" ++
        "\x00\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x0f\x00\x00\x00\x00\x00\x00\x00" ++
        "\x05\x00\x00\x00";

    var stream = std.io.fixedBufferStream(buffer);
    const reader = stream.reader();

    var table = try HashTable(u32, IndexContext).read(testing.allocator, reader);
    defer table.deinit(testing.allocator);

    try testing.expectEqual(buffer.len, table.serializedSize());

    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try output.ensureTotalCapacityPrecise(table.serializedSize());
    try table.write(output.writer());

    try testing.expectEqualStrings(buffer, output.items);
}

test "roundtrip test - compatibility with MSVC" {
    // Since our HashTable implementation is based on that of LLVM's,
    // we differ slightly in how we lower the bitvectors compared to MSVC.
    const buffer: []const u8 =
        "\x06\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x01\x00\x00\x00\x6f\x00\x00\x00" ++
        "\x01\x00\x00\x00\x00\x00\x00\x00" ++
        "\x2b\x00\x00\x00\x89\x01\x00\x00" ++
        "\x1a\x00\x00\x00\x87\x01\x00\x00" ++
        "\x00\x00\x00\x00\x05\x00\x00\x00" ++
        "\x0a\x00\x00\x00\x06\x00\x00\x00" ++
        "\x13\x00\x00\x00\x07\x00\x00\x00" ++
        "\x4a\x00\x00\x00\x8c\x01\x00\x00";

    var stream = std.io.fixedBufferStream(buffer);
    const reader = stream.reader();

    var table = try HashTable(u32, IndexContext).read(testing.allocator, reader);
    defer table.deinit(testing.allocator);

    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try output.ensureTotalCapacityPrecise(table.serializedSize());
    try table.write(output.writer());

    const expected: []const u8 =
        "\x06\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x01\x00\x00\x00\x6f\x00\x00\x00" ++
        "\x00\x00\x00\x00\x2b\x00\x00\x00" ++
        "\x89\x01\x00\x00\x1a\x00\x00\x00" ++
        "\x87\x01\x00\x00\x00\x00\x00\x00" ++
        "\x05\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x06\x00\x00\x00\x13\x00\x00\x00" ++
        "\x07\x00\x00\x00\x4a\x00\x00\x00" ++
        "\x8c\x01\x00\x00";

    try testing.expectEqualStrings(expected, output.items);
}

const IndexContext = struct {
    strtab: []const u8,

    pub fn hash(ctx: @This(), key: u32) u32 {
        const slice = mem.sliceTo(@as([*:0]const u8, @ptrCast(ctx.strtab.ptr)) + key, 0);
        // It is a bug not to truncate a valid u32 to u16.
        return @as(u16, @truncate(hashStringV1(slice)));
    }

    pub fn eql(ctx: @This(), key1: u32, key2: u32) bool {
        _ = ctx;
        return key1 == key2;
    }
};

const IndexAdapter = struct {
    strtab: []const u8,

    pub fn hash(ctx: @This(), key: []const u8) u32 {
        _ = ctx;
        // It is a bug not to truncate a valid u32 to u16.
        return @as(u16, @truncate(hashStringV1(key)));
    }

    pub fn eql(ctx: @This(), key1: []const u8, key2: u32) bool {
        const slice = mem.sliceTo(@as([*:0]const u8, @ptrCast(ctx.strtab.ptr)) + key2, 0);
        return mem.eql(u8, key1, slice);
    }
};

fn addStringToStrtab(strtab: *std.ArrayList(u8), bytes: []const u8) !u32 {
    const offset: u32 = @intCast(strtab.items.len);
    try strtab.ensureUnusedCapacity(bytes.len + 1);
    strtab.appendSliceAssumeCapacity(bytes);
    strtab.appendAssumeCapacity(0);
    return offset;
}

const TestWrapper = struct {
    strtab: *std.ArrayList(u8),
    hash_table: *HashTable(u32, IndexContext),

    fn addString(self: @This(), bytes: []const u8) !u32 {
        return addStringToStrtab(self.strtab, bytes);
    }

    fn get(self: @This(), key: []const u8) ?u32 {
        return self.hash_table.get(key, IndexAdapter{ .strtab = self.strtab.items });
    }

    fn put(self: @This(), key: []const u8, value: u32) !void {
        const gop = try self.hash_table.getOrPut(
            testing.allocator,
            key,
            IndexAdapter{ .strtab = self.strtab.items },
            IndexContext{ .strtab = self.strtab.items },
        );
        if (gop.found_existing) {
            gop.value_ptr.* = value;
            return;
        }

        gop.key_ptr.* = try self.addString(key);
        gop.value_ptr.* = value;
    }
};

test "getIndex" {
    const buffer: []const u8 =
        "\x02\x00\x00\x00\x04\x00\x00\x00" ++
        "\x01\x00\x00\x00\x06\x00\x00\x00" ++
        "\x00\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x0f\x00\x00\x00\x00\x00\x00\x00" ++
        "\x05\x00\x00\x00";

    var stream = std.io.fixedBufferStream(buffer);
    const reader = stream.reader();

    var table = try HashTable(u32, IndexContext).read(testing.allocator, reader);
    defer table.deinit(testing.allocator);

    var strtab = std.ArrayList(u8).init(testing.allocator);
    defer strtab.deinit();

    _ = try addStringToStrtab(&strtab, "/LinkInfo");
    _ = try addStringToStrtab(&strtab, "/names");

    const key_ctx = IndexAdapter{ .strtab = strtab.items };

    var res = table.getIndex("/names", key_ctx);
    try testing.expect(res != null);
    try testing.expectEqual(table.buckets.items[res.?].value, 0xf);

    res = table.getIndex("/LinkInfo", key_ctx);
    try testing.expect(res != null);
    try testing.expectEqual(table.buckets.items[res.?].value, 0x5);

    res = table.getIndex("/TMCache", key_ctx);
    try testing.expect(res == null);
}

test "grow" {
    var table = HashTable(u32, IndexContext){};
    defer table.deinit(testing.allocator);

    var strtab = std.ArrayList(u8).init(testing.allocator);
    defer strtab.deinit();

    const wrapper = TestWrapper{
        .strtab = &strtab,
        .hash_table = &table,
    };

    try testing.expectEqual(@as(u32, 0), table.capacity());

    try wrapper.put("/LinkInfo", 0x0);
    try testing.expectEqual(@as(u32, 2), table.capacity());
    try testing.expect(table.count() < HashTable(u32, IndexContext).maxLoad(table.capacity()));

    try wrapper.put("/names", 0xf);
    try testing.expectEqual(@as(u32, 4), table.capacity());
    try testing.expect(table.count() < HashTable(u32, IndexContext).maxLoad(table.capacity()));

    try wrapper.put("/TMCache", 0xf);
    try testing.expectEqual(@as(u32, 5), table.capacity());
    try testing.expect(table.count() < HashTable(u32, IndexContext).maxLoad(table.capacity()));

    try wrapper.put("/sources", 0xf);
    try testing.expectEqual(@as(u32, 7), table.capacity());
    try testing.expect(table.count() < HashTable(u32, IndexContext).maxLoad(table.capacity()));
}

test "basic operations" {
    var table = HashTable(u32, IndexContext){};
    defer table.deinit(testing.allocator);

    var strtab = std.ArrayList(u8).init(testing.allocator);
    defer strtab.deinit();

    const wrapper = TestWrapper{
        .hash_table = &table,
        .strtab = &strtab,
    };

    try wrapper.put("/LinkInfo", 0x5);
    try wrapper.put("/names", 0xf);

    var value = wrapper.get("/names");
    try testing.expect(value != null);
    try testing.expectEqual(value.?, 0xf);

    value = wrapper.get("/LinkInfo");
    try testing.expect(value != null);
    try testing.expectEqual(value.?, 0x5);

    value = wrapper.get("/TMCache");
    try testing.expect(value == null);

    const expected: []const u8 =
        "\x02\x00\x00\x00\x04\x00\x00\x00" ++
        "\x01\x00\x00\x00\x06\x00\x00\x00" ++
        "\x00\x00\x00\x00\x00\x00\x00\x00" ++
        "\x05\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x0f\x00\x00\x00";

    try testing.expectEqual(expected.len, table.serializedSize());

    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try output.ensureTotalCapacityPrecise(table.serializedSize());

    try table.write(output.writer());

    try testing.expectEqualSlices(u8, expected, output.items);
}
