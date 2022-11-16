const std = @import("std");
const assert = std.debug.assert;
const log = std.log;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const MsfStream = @import("PdbDump.zig").MsfStream;

/// Corresponds to LLVM's `hashStringV1` and Microsoft's `Hasher::lhashPbCb`.
/// This hash is used for name hash table (such as NamedStreamMap) and TPI/IPI hashes.
pub fn hashStringV1(str: []const u8) u32 {
    var result: u32 = 0;

    const longs = @ptrCast([*]align(1) const u32, str.ptr)[0..@divTrunc(str.len, 4)];
    for (longs) |val| {
        result ^= val;
    }

    var remainder_pos = longs.len * @sizeOf(u32);
    var remainder_size = str.len % 4;

    if (remainder_size >= 2) {
        const val = @ptrCast(*align(1) const u16, str.ptr + remainder_pos).*;
        result ^= val;
        remainder_pos += 2;
        remainder_size -= 2;
    }

    if (remainder_size == 1) {
        result ^= str[remainder_pos];
    }

    const to_lower_mask = 0x20202020;
    result |= to_lower_mask;
    result ^= (result >> 11);
    result ^= (result >> 16);

    return result;
}

/// Corresponds to LLVM's `hashStringV2` and Microsoft's `HasherV2::HashULONG`.
/// This hash is used for name hash table.
pub fn hashStringV2(str: []const u8) u32 {
    var result: u32 = 0xb170a1bf;

    const items = @ptrCast([*]align(1) const u32, str.ptr)[0..@divTrunc(str.len, @sizeOf(u32))];
    for (items) |item| {
        result +%= item;
        result +%= (result << 10);
        result ^= (result >> 6);
    }

    const remainder = str[items.len * @sizeOf(u32) ..];
    for (remainder) |item| {
        result +%= item;
        result +%= (result << 10);
        result ^= (result >> 6);
    }

    return result *% 1664525 +% 1013904223;
}

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
///     exceed `capacity * 2 / 3 + 1`.
///
/// For more info, see:
/// https://llvm.org/docs/PDB/HashTable.html
pub fn HashTable(comptime Value: type) type {
    return struct {
        header: Header,
        buckets: std.ArrayListUnmanaged(Entry) = .{},
        present: DynamicBitSetUnmanaged = .{},
        deleted: DynamicBitSetUnmanaged = .{},

        const Self = @This();

        const Header = extern struct {
            size: u32,
            capacity: u32,
        };

        pub const Entry = extern struct {
            key: u32,
            value: Value,
        };

        pub fn init(gpa: Allocator) error{OutOfMemory}!Self {
            return initWithCapacity(gpa, 8);
        }

        pub fn initWithCapacity(gpa: Allocator, capacity: u32) error{OutOfMemory}!Self {
            assert(capacity > 0);
            var self = Self{ .header = .{
                .size = 0,
                .capacity = capacity,
            } };
            try self.buckets.resize(gpa, self.header.capacity);
            self.present = try DynamicBitSetUnmanaged.initEmpty(gpa, self.header.capacity);
            self.deleted = try DynamicBitSetUnmanaged.initEmpty(gpa, self.header.capacity);
            return self;
        }

        pub fn deinit(self: *Self, gpa: Allocator) void {
            self.buckets.deinit(gpa);
            self.present.deinit(gpa);
            self.deleted.deinit(gpa);
            self.* = undefined;
        }

        /// Returns the number of values present in the HashTable.
        pub fn count(self: Self) u32 {
            return self.header.size;
        }

        /// Returns the corresponding `Value` for `key` if one exists.
        pub fn get(self: Self, comptime Key: type, comptime Context: type, key: Key, ctx: Context) ?Value {
            comptime verifyContext(Key, Context);
            const res = self.getIndexOrFirstUnused(Key, Context, key, ctx);
            return if (res.existing) self.buckets.items[res.index].value else null;
        }

        /// Inserts or updates value for key `key`.
        pub fn put(
            self: *Self,
            comptime Key: type,
            comptime Context: type,
            gpa: Allocator,
            key: Key,
            value: Value,
            ctx: Context,
        ) PutError(Context)!void {
            comptime verifyContext(Key, Context);
            return self.putInternal(Key, Context, gpa, key, value, null, ctx);
        }

        /// Calculates required number of bytes to serialize the HashTable
        /// to a byte stream.
        /// Use it to preallocate the output buffer.
        pub fn serializedSize(self: Self) usize {
            var size: usize = @sizeOf(Header);

            for (&[_]DynamicBitSetUnmanaged{ self.present, self.deleted }) |vec| {
                if (findLastSet(vec)) |index| {
                    size += numMasks(u32, index + 1) * @sizeOf(u32);
                }
                size += @sizeOf(u32);
            }

            size += self.count() * @sizeOf(Entry);

            return size;
        }

        /// Reads the HashTable from an input stream.
        /// HashTable is serialized according to the PDB spec found at:
        /// https://llvm.org/docs/PDB/HashTable.html
        pub fn read(gpa: Allocator, reader: anytype) !Self {
            const header = try reader.readStruct(Header);

            var self = Self{ .header = header };

            var present = try readBitSet(u32, gpa, reader);
            defer present.deinit(gpa);

            if (present.count() != self.header.size) {
                log.err("Present bit vector does not the match size of hash table", .{});
                return error.InvalidHashMap;
            }

            var deleted = try readBitSet(u32, gpa, reader);
            defer deleted.deinit(gpa);

            if (intersects(present, deleted)) {
                log.err("Buckets marked as both valid and deleted", .{});
                return error.InvalidHashMap;
            }

            const bit_length = @max(present.bit_length, deleted.bit_length);

            self.present = try DynamicBitSetUnmanaged.initEmpty(gpa, bit_length);
            {
                var i: usize = 0;
                while (i < numMasks(usize, present.bit_length)) : (i += 1) {
                    self.present.masks[i] = present.masks[i];
                }
            }

            self.deleted = try DynamicBitSetUnmanaged.initEmpty(gpa, bit_length);
            {
                var i: usize = 0;
                while (i < numMasks(usize, deleted.bit_length)) : (i += 1) {
                    self.deleted.masks[i] = deleted.masks[i];
                }
            }

            try self.buckets.resize(gpa, header.capacity);

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
            try writer.writeAll(@ptrCast([*]const u8, &self.header)[0..@sizeOf(Header)]);

            for (&[_]DynamicBitSetUnmanaged{ self.present, self.deleted }) |vec| {
                // Calculate number of words for the bit vector
                const present_num_words: u32 = if (findLastSet(vec)) |index|
                    @intCast(u32, numMasks(u32, index + 1))
                else
                    0;
                try writer.writeIntLittle(u32, present_num_words);

                // Serialize the sequence of bitvector's masks
                if (present_num_words > 0) {
                    const present_words = @ptrCast([*]const u32, vec.masks)[0..present_num_words];
                    for (present_words) |word| {
                        try writer.writeIntLittle(u32, word);
                    }
                }
            }

            // Finally, serialize valid (present) buckets in the bucket list
            var it = self.present.iterator(.{});
            while (it.next()) |index| {
                const entry = self.buckets.items[index];
                try writer.writeIntLittle(u32, entry.key);
                try writer.writeAll(@ptrCast([*]const u8, &entry.value)[0..@sizeOf(Value)]);
            }
        }

        fn verifyContext(comptime Key: type, comptime Context: type) void {
            switch (@typeInfo(Context)) {
                .Struct => {},
                else => |other| @compileError("Invalid Context: expected struct, found " ++ @typeName(other)),
            }

            if (@hasDecl(Context, "hash")) {
                verifyMethod("hash", Context.hash, &[_]type{ Context, Key }, .{ .tt = u32 });
            } else @compileError("missing required method 'hash'");

            if (@hasDecl(Context, "eql")) {
                verifyMethod("eql", Context.eql, &[_]type{ Context, Key, Key }, .{ .tt = bool });
            } else @compileError("missing required method 'eql'");

            if (@hasDecl(Context, "getKeyAdapted")) {
                verifyMethod("getKeyAdapted", Context.getKeyAdapted, &[_]type{ Context, u32 }, .{
                    .tag = .optional,
                    .tt = Key,
                });
            } else @compileError("missing required method 'getKeyAdapted'");

            if (@hasDecl(Context, "putKeyAdapted")) {
                verifyMethod("putKeyAdapted", Context.putKeyAdapted, &[_]type{ Context, Key }, .{
                    .tag = .error_union,
                    .tt = u32,
                });
            } else @compileError("missing required method 'putKeyAdapted'");
        }

        const Ret = struct {
            tag: enum { none, optional, error_union } = .none,
            tt: type,
        };

        fn verifyMethod(
            comptime name: []const u8,
            comptime Fn: anytype,
            comptime params: []const type,
            comptime ret: Ret,
        ) void {
            const func = switch (@typeInfo(@TypeOf(Fn))) {
                .Fn => |func| func,
                else => |other| @compileError(name ++ ": expected a method, found " ++ @typeName(other)),
            };

            const ret_type = func.return_type.?;
            blk: {
                switch (ret.tag) {
                    .none => {
                        if (ret.tt == ret_type) break :blk;
                        @compileError(
                            name ++ ": expected return type of " ++ @typeName(ret.tt) ++ ", found " ++ @typeName(ret_type),
                        );
                    },
                    .optional => {
                        switch (@typeInfo(ret_type)) {
                            .Optional => |ti| if (ti.child == ret.tt) break :blk,
                            else => {},
                        }
                        @compileError(
                            name ++ ": expected return type of ?" ++ @typeName(ret.tt) ++ ", found " ++ @typeName(ret_type),
                        );
                    },
                    .error_union => {
                        switch (@typeInfo(ret_type)) {
                            .ErrorUnion => |ti| if (ti.payload == ret.tt) break :blk,
                            else => {},
                        }
                        @compileError(
                            name ++ ": expected return type of !" ++ @typeName(ret.tt) ++ ", found " ++ @typeName(ret_type),
                        );
                    },
                }
            }

            if (func.args.len != params.len) {
                @compileError(name ++ ": expected " ++ params.len ++ " parameters, found " ++ func.args.len);
            }

            inline for (params) |exp, i| {
                const given = func.args[i].arg_type.?;
                if (exp != given) {
                    @compileError(
                        name ++ ": expected param " ++ i ++ " of type " ++ @typeName(exp) ++ ", found " ++ @typeName(given),
                    );
                }
            }
        }

        const GetIndexOrFirstUnused = struct {
            existing: bool,
            index: u32,
        };

        /// If the `key` exists in the HashTable, returns `index` to the bucket holding the `value`.
        /// In this case, `existing` is set to `true`.
        /// If the `key` doesn't exist, returns `index` to the first available bucket.
        /// In this case, `existing` is set to `false`.
        fn getIndexOrFirstUnused(
            self: Self,
            comptime Key: type,
            comptime Context: type,
            key: Key,
            ctx: Context,
        ) GetIndexOrFirstUnused {
            const hash_bucket = ctx.hash(key) % self.header.capacity;
            var index = hash_bucket;
            var first_unused: ?u32 = null;

            while (true) {
                if (self.present.isSet(index)) {
                    if (ctx.getKeyAdapted(self.buckets.items[index].key)) |okey| {
                        if (ctx.eql(okey, key)) {
                            return .{
                                .existing = true,
                                .index = index,
                            };
                        }
                    }
                } else {
                    if (first_unused == null) {
                        first_unused = index;
                    }

                    if (!self.deleted.isSet(index)) {
                        break;
                    }
                }

                index = (index + 1) % self.header.capacity;
                if (index == hash_bucket) break;
            }

            assert(first_unused != null);
            return .{
                .existing = false,
                .index = first_unused.?,
            };
        }

        fn PutError(comptime Context: type) type {
            if (@hasDecl(Context, "putKeyAdapted")) {
                const ret = @typeInfo(@TypeOf(Context.putKeyAdapted)).Fn.return_type.?;
                const err = @typeInfo(ret).ErrorUnion.error_set;
                return error{OutOfMemory} || err;
            }
            @compileError("putKeyAdapted method not found");
        }

        fn putInternal(
            self: *Self,
            comptime Key: type,
            comptime Context: type,
            gpa: Allocator,
            key: Key,
            value: Value,
            adapted: ?u32,
            ctx: Context,
        ) PutError(Context)!void {
            const res = self.getIndexOrFirstUnused(Key, Context, key, ctx);
            const entry = &self.buckets.items[res.index];
            if (res.existing) {
                assert(self.present.isSet(res.index));
                assert(ctx.eql(ctx.getKeyAdapted(entry.key).?, key));
                entry.value = value;
            } else {
                assert(!self.present.isSet(res.index));
                entry.key = adapted orelse try ctx.putKeyAdapted(key);
                entry.value = value;

                self.present.set(res.index);
                self.deleted.unset(res.index);
                self.header.size += 1;

                if (self.header.size >= maxLoad(self.header.capacity)) {
                    try self.grow(Key, Context, gpa, ctx);
                }
            }
        }

        fn grow(
            self: *Self,
            comptime Key: type,
            comptime Context: type,
            gpa: Allocator,
            ctx: Context,
        ) PutError(Context)!void {
            assert(self.header.capacity < std.math.maxInt(u32)); // Capacity at max, cannot grow!

            const new_capacity = if (self.header.capacity <= std.math.maxInt(u32))
                maxLoad(self.header.capacity) * 2
            else
                std.math.maxInt(u32);

            var table = try Self.initWithCapacity(gpa, new_capacity);
            defer table.deinit(gpa);

            var present = self.present.iterator(.{});
            while (present.next()) |index| {
                const entry = self.buckets.items[index];
                const key = ctx.getKeyAdapted(entry.key).?;
                try table.putInternal(Key, Context, gpa, key, entry.value, entry.key, ctx);
            }

            mem.swap(Self, self, &table);
        }

        inline fn maxLoad(capacity: u32) u32 {
            return capacity * 2 / 3 + 1;
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

fn readBitSet(comptime Word: type, gpa: Allocator, reader: anytype) !DynamicBitSetUnmanaged {
    const num_words = try reader.readIntLittle(Word);
    const bit_length = num_words * @bitSizeOf(Word);
    var bitset = try DynamicBitSetUnmanaged.initEmpty(gpa, bit_length);

    var i: usize = 0;
    while (i < num_words) : (i += 1) {
        const word = try reader.readIntLittle(Word);
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

    var table = try HashTable(u32).read(testing.allocator, reader);
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

    var table = try HashTable(u32).read(testing.allocator, reader);
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

const StringContext = struct {
    strtab: *std.ArrayList(u8),

    pub fn hash(ctx: @This(), key: []const u8) u32 {
        _ = ctx;
        return @truncate(u16, hashStringV1(key));
    }

    pub fn eql(ctx: @This(), key1: []const u8, key2: []const u8) bool {
        _ = ctx;
        return mem.eql(u8, key1, key2);
    }

    pub fn getKeyAdapted(ctx: @This(), adapted: u32) ?[]const u8 {
        if (adapted > ctx.strtab.items.len) return null;
        return mem.sliceTo(@ptrCast([*:0]u8, ctx.strtab.items.ptr + adapted), 0);
    }

    pub fn putKeyAdapted(ctx: @This(), key: []const u8) error{OutOfMemory}!u32 {
        const adapted = @intCast(u32, ctx.strtab.items.len);
        try ctx.strtab.ensureUnusedCapacity(key.len + 1);
        ctx.strtab.appendSliceAssumeCapacity(key);
        ctx.strtab.appendAssumeCapacity(0);
        return adapted;
    }
};

test "getIndexOrFirstUnused" {
    const buffer: []const u8 =
        "\x02\x00\x00\x00\x04\x00\x00\x00" ++
        "\x01\x00\x00\x00\x06\x00\x00\x00" ++
        "\x00\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x0f\x00\x00\x00\x00\x00\x00\x00" ++
        "\x05\x00\x00\x00";

    var stream = std.io.fixedBufferStream(buffer);
    const reader = stream.reader();

    var table = try HashTable(u32).read(testing.allocator, reader);
    defer table.deinit(testing.allocator);

    var strtab = std.ArrayList(u8).init(testing.allocator);
    defer strtab.deinit();

    const ctx = StringContext{ .strtab = &strtab };

    {
        const off = try ctx.putKeyAdapted("/LinkInfo");
        try testing.expectEqual(@as(u32, 0), off);
    }
    {
        const off = try ctx.putKeyAdapted("/names");
        try testing.expectEqual(@as(u32, 0xa), off);
    }

    var res = table.getIndexOrFirstUnused([]const u8, StringContext, "/names", ctx);
    try testing.expect(res.existing);
    try testing.expect(ctx.getKeyAdapted(table.buckets.items[res.index].key) != null);
    try testing.expectEqualStrings(ctx.getKeyAdapted(table.buckets.items[res.index].key).?, "/names");
    try testing.expectEqual(table.buckets.items[res.index].value, 0xf);

    res = table.getIndexOrFirstUnused([]const u8, StringContext, "/LinkInfo", ctx);
    try testing.expect(res.existing);
    try testing.expect(ctx.getKeyAdapted(table.buckets.items[res.index].key) != null);
    try testing.expectEqualStrings(ctx.getKeyAdapted(table.buckets.items[res.index].key).?, "/LinkInfo");
    try testing.expectEqual(table.buckets.items[res.index].value, 0x5);

    res = table.getIndexOrFirstUnused([]const u8, StringContext, "/TMCache", ctx);
    try testing.expect(!res.existing);
}

test "grow" {
    var table = try HashTable(u32).initWithCapacity(testing.allocator, 1);
    defer table.deinit(testing.allocator);

    var strtab = std.ArrayList(u8).init(testing.allocator);
    defer strtab.deinit();

    const ctx = StringContext{ .strtab = &strtab };

    try testing.expectEqual(@as(u32, 1), table.header.capacity);

    try table.put([]const u8, StringContext, testing.allocator, "/LinkInfo", 0x5, ctx);
    try testing.expectEqual(@as(u32, 2), table.header.capacity);
    try testing.expect(table.header.size < HashTable(u32).maxLoad(table.header.capacity));

    try table.put([]const u8, StringContext, testing.allocator, "/names", 0xf, ctx);
    try testing.expectEqual(@as(u32, 4), table.header.capacity);
    try testing.expect(table.header.size < HashTable(u32).maxLoad(table.header.capacity));

    try table.put([]const u8, StringContext, testing.allocator, "/TMCache", 0x6, ctx);
    try testing.expectEqual(@as(u32, 6), table.header.capacity);
    try testing.expect(table.header.size < HashTable(u32).maxLoad(table.header.capacity));

    try table.put([]const u8, StringContext, testing.allocator, "/sources", 0xe, ctx);
    try testing.expectEqual(@as(u32, 6), table.header.capacity);
    try testing.expect(table.header.size < HashTable(u32).maxLoad(table.header.capacity));
}

test "basic operations" {
    var table = try HashTable(u32).initWithCapacity(testing.allocator, 1);
    defer table.deinit(testing.allocator);

    var strtab = std.ArrayList(u8).init(testing.allocator);
    defer strtab.deinit();

    const ctx = StringContext{ .strtab = &strtab };

    try table.put([]const u8, StringContext, testing.allocator, "/LinkInfo", 0x5, ctx);
    try table.put([]const u8, StringContext, testing.allocator, "/names", 0xf, ctx);

    var value = table.get([]const u8, StringContext, "/names", ctx);
    try testing.expect(value != null);
    try testing.expectEqual(value.?, 0xf);

    value = table.get([]const u8, StringContext, "/LinkInfo", ctx);
    try testing.expect(value != null);
    try testing.expectEqual(value.?, 0x5);

    value = table.get([]const u8, StringContext, "/TMCache", ctx);
    try testing.expect(value == null);

    const expected: []const u8 =
        "\x02\x00\x00\x00\x04\x00\x00\x00" ++
        "\x01\x00\x00\x00\x06\x00\x00\x00" ++
        "\x00\x00\x00\x00\x0a\x00\x00\x00" ++
        "\x0f\x00\x00\x00\x00\x00\x00\x00" ++
        "\x05\x00\x00\x00";

    try testing.expectEqual(expected.len, table.serializedSize());

    var output = std.ArrayList(u8).init(testing.allocator);
    defer output.deinit();
    try output.ensureTotalCapacityPrecise(table.serializedSize());

    try table.write(output.writer());

    try testing.expectEqualStrings(expected, output.items);
}
