const std = @import("std");
const assert = std.debug.assert;
const log = std.log;

const Allocator = std.mem.Allocator;
const DynamicBitSetUnmanaged = std.bit_set.DynamicBitSetUnmanaged;
const MsfStream = @import("PdbDump.zig").MsfStream;

pub fn HashTable(comptime Value: type) type {
    return struct {
        gpa: Allocator,
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

        pub fn deinit(self: *Self) void {
            self.buckets.deinit(self.gpa);
            self.present.deinit(self.gpa);
            self.deleted.deinit(self.gpa);
        }

        pub fn count(self: Self) usize {
            return self.present.count();
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

            var self = Self{
                .gpa = gpa,
                .header = header,
            };

            try readBitSet(u32, gpa, &self.present, reader);

            if (self.present.count() != self.header.size) {
                log.err("Present bit vector does not the match size of hash table", .{});
                return error.InvalidHashMap;
            }

            try readBitSet(u32, gpa, &self.deleted, reader);

            if (intersects(&self.present, &self.deleted)) {
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

fn intersects(lhs: *const DynamicBitSetUnmanaged, rhs: *const DynamicBitSetUnmanaged) bool {
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
