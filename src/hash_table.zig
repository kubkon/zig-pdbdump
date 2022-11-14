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

        pub fn readFromMsfStream(stream: MsfStream, gpa: Allocator, pos: usize) !Self {
            var curr = pos;
            const header = try stream.read(Header, curr);
            curr += @sizeOf(Header);

            var present = try readBitSet(stream, gpa, curr);
            errdefer present.deinit(gpa);
            curr += (BitSet.numMasks(present.bit_length) + 1) * @sizeOf(u32);

            var deleted = try readBitSet(stream, gpa, curr);
            errdefer deleted.deinit(gpa);
            curr += (BitSet.numMasks(deleted.bit_length) + 1) * @sizeOf(u32);

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

                var entry = try stream.read(Entry, curr);
                curr += @sizeOf(Entry);

                self.buckets.items[index] = entry;
            }

            return self;
        }
    };
}

fn readBitSet(stream: MsfStream, gpa: Allocator, pos: usize) !BitSet {
    const num_masks = try stream.read(u32, pos);
    const raw_bytes = try stream.bytesAlloc(gpa, pos + @sizeOf(u32), num_masks * @sizeOf((u32)));
    defer gpa.free(raw_bytes);

    const bit_length = num_masks * @bitSizeOf(u32);
    var bit_set = try BitSet.initEmpty(gpa, bit_length);

    const masks = @ptrCast([*]align(1) const u32, raw_bytes.ptr)[0..num_masks];
    for (masks) |mask, i| {
        bit_set.masks[i] = mask;
    }

    return bit_set;
}
