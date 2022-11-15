const std = @import("std");
const assert = std.debug.assert;
const log = std.log;
const mem = std.mem;
const testing = std.testing;

const Allocator = mem.Allocator;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const MsfStream = @import("PdbDump.zig").MsfStream;

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

pub fn HashTable(comptime Value: type) type {
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

        pub const Entry = extern struct {
            key: u32,
            value: Value,
        };

        pub fn deinit(self: *Self, gpa: Allocator) void {
            self.buckets.deinit(gpa);
            self.present.deinit(gpa);
            self.deleted.deinit(gpa);
        }

        pub fn count(self: Self) usize {
            return self.present.count();
        }

        const GetIndexOrFirstUnused = struct {
            existing: bool,
            index: u32,
        };

        pub fn getIndexOrFirstUnused(
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
                    if (ctx.invHash(self.buckets.items[index].key)) |okey| {
                        if (mem.eql(u8, okey, key)) {
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

        pub fn get(self: Self, comptime Key: type, comptime Context: type, key: Key, ctx: Context) Value {
            const res = self.getIndexOrFirstUnused(Key, Context, key, ctx);
            assert(res.existing);
            return self.buckets.items[res.index].value;
        }

        inline fn numMasks(comptime Word: type, bit_length: usize) usize {
            return (bit_length + (@bitSizeOf(Word) - 1)) / @bitSizeOf(Word);
        }

        pub fn serializedSize(self: Self) usize {
            var size: usize = @sizeOf(Header);

            if (findLastSet(self.present)) |index| {
                size += numMasks(u32, index + 1) * @sizeOf(u32);
            }
            size += @sizeOf(u32);

            if (findLastSet(self.deleted)) |index| {
                size += numMasks(u32, index + 1) * @sizeOf(u32);
            }
            size += @sizeOf(u32);

            size += self.count() * @sizeOf(Entry);

            return size;
        }

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

test "roundtrip test - panic.pdb" {
    const buffer: []const u8 =
        "\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00" ++
        "\x06\x00\x00\x00\x00\x00\x00\x00\n\x00\x00\x00" ++
        "\x0f\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00";

    var stream = std.io.fixedBufferStream(buffer);
    const reader = stream.reader();

    var table = try HashTable(u32).read(testing.allocator, reader);
    defer table.deinit(testing.allocator);

    try testing.expectEqual(buffer.len, table.serializedSize());
}

test "roundtrip test - simple.pdb" {
    const buffer: []const u8 = "\x06\x00\x00\x00\n\x00\x00\x00\x01\x00\x00\x00o\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00+\x00\x00\x00\x89\x01\x00\x00\x1a\x00\x00\x00\x87\x01\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\n\x00\x00\x00\x06\x00\x00\x00\x13\x00\x00\x00\x07\x00\x00\x00J\x00\x00\x00\x8c\x01\x00\x00";

    var stream = std.io.fixedBufferStream(buffer);
    const reader = stream.reader();

    var table = try HashTable(u32).read(testing.allocator, reader);
    defer table.deinit(testing.allocator);

    try testing.expectEqual(buffer.len - 4, table.serializedSize());
}
