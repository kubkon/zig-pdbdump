const std = @import("std");
const assert = std.debug.assert;
const hashStringV1 = @import("hash.zig").hashStringV1;
const log = std.log;
const math = std.math;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;

/// Represents the PDB serializable hash set / reverse lookup.
pub fn HashSet(comptime Context: type) type {
    return struct {
        header: Header = .{ .size = 0, .capacity = 0 },
        buckets: std.ArrayListUnmanaged(Key) = .{},
        present: DynamicBitSetUnmanaged = .{},

        const Self = @This();

        const Header = extern struct {
            size: u32,
            capacity: u32,
        };

        pub const Key = u32;

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
        pub fn get(self: Self, key: anytype, ctx: anytype) ?Key {
            const index = self.getIndex(key, ctx) orelse return null;
            return self.buckets.items[index];
        }

        const GetOrPutResult = struct {
            found_existing: bool,
            ptr: *Key,
        };

        /// If the key exists, returns pointers to that bucket.
        /// Otherwise, finds the first empty bucket and returns a pointer to it.
        /// If the key did not exist, it will *not* be set in the bucket's ptr -
        /// the caller needs to this once this function returns.
        pub fn getOrPut(
            self: *Self,
            allocator: Allocator,
            key: anytype,
            key_ctx: anytype,
            ctx: Context,
        ) !GetOrPutResult {
            if (self.getIndex(key, key_ctx)) |index| {
                assert(self.present.isSet(index));
                assert(key_ctx.eql(key, self.buckets.items[index]));
                return GetOrPutResult{
                    .found_existing = true,
                    .ptr = &self.buckets.items[index],
                };
            }

            if (self.count() + 1 > maxLoad(self.capacity())) {
                try self.grow(allocator, capacityForSize(self.count() + 1), ctx);
            }

            return self.getOrPutAssumeCapacity(key, key_ctx);
        }

        pub fn getOrPutAssumeCapacity(self: *Self, key: anytype, ctx: anytype) GetOrPutResult {
            const hash_bucket = ctx.hash(key) % self.capacity();
            var index = hash_bucket;
            var found_unused = false;

            while (true) {
                if (self.present.isSet(index)) {
                    if (ctx.eql(key, self.buckets.items[index])) {
                        return .{
                            .found_existing = true,
                            .ptr = &self.buckets.items[index],
                        };
                    }
                } else {
                    found_unused = true;
                    break;
                }

                index = (index + 1) % self.capacity();
                if (index == hash_bucket) break;
            }

            assert(found_unused); // Not enough capacity!
            self.present.set(index);
            self.header.size += 1;

            return .{
                .found_existing = false,
                .ptr = &self.buckets.items[index],
            };
        }

        /// Calculates required number of bytes to serialize the HashSet
        /// to a byte stream.
        /// Use it to preallocate the output buffer.
        pub fn serializedSize(self: Self) usize {
            return @sizeOf(Header) + self.capacity() * @sizeOf(Key);
        }

        /// Reads the HashSet from an input stream.
        pub fn read(allocator: Allocator, reader: anytype) !Self {
            const cap = try reader.readIntLittle(u32);

            var self = Self{ .header = .{
                .size = undefined,
                .capacity = cap,
            } };
            try self.allocate(allocator, cap);

            var i: u32 = 0;
            while (i < cap) : (i += 1) {
                const off = try reader.readIntLittle(u32);
                const is_set = off > 0;
                self.buckets.items[i] = off;
                if (is_set) {
                    self.present.set(i);
                }
            }

            const size = try reader.readIntLittle(u32);
            self.header.size = size;

            if (size != self.present.count()) {
                log.err("present buckets do not match specified size: 0x{x} != 0x{x}", .{ size, self.present.count() });
                return error.InvalidHashSet;
            }

            return self;
        }

        /// Writes the HashSet to an output stream.
        pub fn write(self: Self, writer: anytype) !void {
            try writer.writeIntLittle(u32, self.capacity());

            var i: u32 = 0;
            while (i < self.capacity()) : (i += 1) {
                try writer.writeIntLittle(u32, self.buckets.items[i]);
            }

            try writer.writeIntLittle(u32, self.count());
        }

        pub const Iterator = struct {
            table: *const Self,
            iter: DynamicBitSetUnmanaged.Iterator(.{}),

            pub fn next(it: *Iterator) ?Key {
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
                    if (ctx.eql(key, self.buckets.items[index])) {
                        return index;
                    }
                } else break;

                index = (index + 1) % self.capacity();
                if (index == hash_bucket) break;
            }

            return null;
        }

        fn grow(self: *Self, allocator: Allocator, new_capacity: u32, ctx: Context) !void {
            var set = Self{};
            defer set.deinit(allocator);
            try set.allocate(allocator, new_capacity);
            assert(set.capacity() == new_capacity);

            var present = self.present.iterator(.{});
            while (present.next()) |index| {
                const key = self.buckets.items[index];
                const gop = set.getOrPutAssumeCapacity(key, ctx);
                assert(!gop.found_existing);
                gop.ptr.* = key;
            }

            mem.swap(Self, self, &set);
        }

        fn allocate(self: *Self, allocator: Allocator, new_capacity: u32) !void {
            self.header = .{
                .size = 0,
                .capacity = new_capacity,
            };
            try self.buckets.resize(allocator, self.capacity());
            mem.set(Key, self.buckets.items, 0);
            self.present = try DynamicBitSetUnmanaged.initEmpty(allocator, self.capacity());
        }

        fn deallocate(self: *Self, allocator: Allocator) void {
            self.buckets.deinit(allocator);
            self.present.deinit(allocator);
        }

        inline fn maxLoad(cap: u32) u32 {
            return cap * 3 / 4;
        }

        fn capacityForSize(size: u32) u32 {
            const min_cap = math.cast(u32, @as(u64, size) * 4 / 3) orelse unreachable;
            return calcNextCapacityValueAtLeast(min_cap);
        }

        fn calcNextCapacityValueAtLeast(min_cap: u32) u32 {
            comptime var cap: u32 = 1;
            inline while (true) {
                if (cap > min_cap) return cap;
                comptime var next_cap = @as(u64, cap) * 3 / 2 + 1;
                cap = comptime math.cast(u32, next_cap) orelse return cap;
            }
        }
    };
}

const IndexContext = struct {
    bytes: []const u8,

    pub fn hash(ctx: @This(), key: u32) u32 {
        const slice = mem.sliceTo(@ptrCast([*:0]const u8, ctx.bytes.ptr) + key, 0);
        return hashStringV1(slice);
    }

    pub fn eql(ctx: @This(), key1: u32, key2: u32) bool {
        _ = ctx;
        return key1 == key2;
    }
};

const IndexAdapter = struct {
    bytes: []const u8,

    pub fn hash(ctx: @This(), key: []const u8) u32 {
        _ = ctx;
        return hashStringV1(key);
    }

    pub fn eql(ctx: @This(), key1: []const u8, key2: u32) bool {
        const slice = mem.sliceTo(@ptrCast([*:0]const u8, ctx.bytes.ptr) + key2, 0);
        return mem.eql(u8, key1, slice);
    }
};

fn addStringToStrtab(strtab: *std.ArrayList(u8), bytes: []const u8) !u32 {
    const offset = @intCast(u32, strtab.items.len);
    try strtab.ensureUnusedCapacity(bytes.len + 1);
    strtab.appendSliceAssumeCapacity(bytes);
    strtab.appendAssumeCapacity(0);
    return offset;
}

const TestWrapper = struct {
    strtab: *std.ArrayList(u8),
    lookup: *HashSet(IndexContext),

    fn addString(self: @This(), bytes: []const u8) !u32 {
        return addStringToStrtab(self.strtab, bytes);
    }

    fn getString(self: @This(), off: u32) []const u8 {
        assert(off < self.strtab.items.len);
        return mem.sliceTo(@ptrCast([*:0]const u8, self.strtab.items.ptr) + off, 0);
    }

    fn get(self: @This(), key: []const u8) ?u32 {
        return self.lookup.get(key, IndexAdapter{ .bytes = self.strtab.items });
    }

    fn put(self: @This(), key: []const u8) !void {
        const gop = try self.lookup.getOrPut(
            testing.allocator,
            key,
            IndexAdapter{ .bytes = self.strtab.items },
            IndexContext{ .bytes = self.strtab.items },
        );
        if (gop.found_existing) {
            return;
        }

        gop.ptr.* = try self.addString(key);
    }
};

test "serialization" {
    const gpa = testing.allocator;

    var set = HashSet(IndexContext){};
    defer set.deinit(gpa);

    var bytes = std.ArrayList(u8).init(gpa);
    defer bytes.deinit();
    try bytes.append(0);

    const wrapper = TestWrapper{
        .strtab = &bytes,
        .lookup = &set,
    };

    try wrapper.put("");

    try testing.expectEqualSlices(u8, "\x00\x00", bytes.items);

    const std_prefix = "C:\\Users\\kubko\\dev\\zig\\stage3\\lib\\zig\\std\\";

    for (&[_][]const u8{
        "target.zig",
        "builtin.zig",
        "start.zig",
        "debug.zig",
    }) |suffix| {
        const path = try std.fmt.allocPrint(gpa, "{s}{s}", .{ std_prefix, suffix });
        defer gpa.free(path);
        try wrapper.put(path);
    }

    try wrapper.put("C:\\Users\\kubko\\dev\\examples\\incremental\\panic.zig");

    for (&[_][]const u8{
        "atomic\\Atomic.zig",
        "fmt.zig",
        "mem.zig",
        "Thread\\Mutex.zig",
        "io.zig",
        "fs\\file.zig",
        "Thread.zig",
        "io\\writer.zig",
        "os.zig",
        "Thread\\Futex.zig",
        "io\\fixed_buffer_stream.zig",
        "os\\windows.zig",
        "io\\reader.zig",
        "math.zig",
        "array_list.zig",
        "coff.zig",
        "mem\\Allocator.zig",
        "dwarf.zig",
        "leb128.zig",
        "io\\seekable_stream.zig",
        "heap\\arena_allocator.zig",
        "fs\\path.zig",
        "process.zig",
        "ascii.zig",
        "pdb.zig",
        "fs.zig",
        "meta.zig",
        "hash_map.zig",
        "unicode.zig",
        "linked_list.zig",
        "hash\\wyhash.zig",
        "heap.zig",
        "meta\\trait.zig",
    }) |suffix| {
        const path = try std.fmt.allocPrint(gpa, "{s}{s}", .{ std_prefix, suffix });
        defer gpa.free(path);
        try wrapper.put(path);
    }

    try testing.expectEqual(@as(u32, 39), set.count());
    try testing.expectEqual(@as(u32, 61), set.capacity());
}
