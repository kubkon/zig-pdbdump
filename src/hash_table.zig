const std = @import("std");
const assert = std.debug.assert;
const log = std.log;
const mem = std.mem;

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

        pub fn get(self: Self, comptime Key: type, comptime Context: type, key: Key, ctx: Context) Value {
            const actual_hash = ctx.hash(key);
            const hash_bucket = actual_hash % self.header.capacity;
            var index = hash_bucket;
            var first_unused: ?u32 = null;

            while (true) {
                if (self.present.capacity() > index and self.present.isSet(index)) {
                    if (ctx.invHash(self.buckets.items[index].key)) |okey| {
                        if (mem.eql(u8, okey, key)) {
                            return self.buckets.items[index].value;
                        }
                    }
                } else {
                    if (first_unused == null) {
                        first_unused = index;
                    }

                    if (self.deleted.capacity() <= index or !self.deleted.isSet(index)) {
                        break;
                    }
                }

                index = (index + 1) % self.header.capacity;
                if (index == hash_bucket) break;
            }

            assert(first_unused != null);
            return self.buckets.items[first_unused.?].value;
        }

        inline fn numMasks(bitset: DynamicBitSetUnmanaged) usize {
            return (bitset.capacity() + (@bitSizeOf(u32) - 1)) / @bitSizeOf(u32);
        }

        pub fn serializedSize(self: Self) usize {
            var size: usize = @sizeOf(Header);
            size += (numMasks(self.present) + 1) * @sizeOf(u32);
            size += (numMasks(self.deleted) + 1) * @sizeOf(u32);
            size += self.count() * @sizeOf(Entry);
            return size;
        }

        pub fn read(gpa: Allocator, reader: anytype) !Self {
            const header = try reader.readStruct(Header);

            var self = Self{ .header = header };

            try readBitSet(u32, gpa, &self.present, reader);

            if (self.present.count() != self.header.size) {
                log.err("Present bit vector does not the match size of hash table", .{});
                return error.InvalidHashMap;
            }

            try readBitSet(u32, gpa, &self.deleted, reader);

            if (intersects(self.present, self.deleted)) {
                log.err("Buckets marked as both valid and deleted", .{});
                return error.InvalidHashMap;
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

fn readBitSet(comptime Word: type, gpa: Allocator, bitset: *DynamicBitSetUnmanaged, reader: anytype) !void {
    const num_words = try reader.readIntLittle(Word);
    const bit_length = num_words * @bitSizeOf(Word);
    bitset.* = try DynamicBitSetUnmanaged.initEmpty(gpa, bit_length);

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
}
