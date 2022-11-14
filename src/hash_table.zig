const std = @import("std");
const assert = std.debug.assert;

const Allocator = std.mem.Allocator;
const BitSet = @import("BitSet.zig");
const MsfStream = @import("PdbDump.zig").MsfStream;

pub fn HashTable(comptime Value: type) type {
    return struct {
        gpa: Allocator,
        header: Header,
        buckets: std.ArrayListUnmanaged(Entry) = .{},
        present: BitSet,
        deleted: BitSet,

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

        pub fn serializedSize(self: Self) usize {
            var size: usize = @sizeOf(Header);
            size += (BitSet.numMasks(self.present.bit_length) + 1) * @sizeOf(u32);
            size += (BitSet.numMasks(self.deleted.bit_length) + 1) * @sizeOf(u32);
            size += self.count() * @sizeOf(Entry);
            return size;
        }

        pub fn read(gpa: Allocator, reader: anytype) !Self {
            const header = try reader.readStruct(Header);

            var present = try readBitSet(gpa, reader);
            errdefer present.deinit(gpa);

            var deleted = try readBitSet(gpa, reader);
            errdefer deleted.deinit(gpa);

            var self = Self{
                .gpa = gpa,
                .header = header,
                .present = present,
                .deleted = deleted,
            };
            try self.buckets.resize(gpa, header.capacity);

            var bucket_it = present.iterator(.{});
            while (bucket_it.next()) |index| {
                if (deleted.capacity() > 0) assert(!deleted.isSet(index)); // TODO convert to an error
                self.buckets.items[index] = try reader.readStruct(Entry);
            }

            return self;
        }
    };
}

fn readBitSet(gpa: Allocator, reader: anytype) !BitSet {
    const num_masks = try reader.readIntLittle(u32);
    const bit_length = num_masks * @bitSizeOf(u32);
    var bit_set = try BitSet.initEmpty(gpa, bit_length);

    var i: usize = 0;
    while (i < num_masks) : (i += 1) {
        bit_set.masks[i] = try reader.readIntLittle(u32);
    }

    return bit_set;
}
