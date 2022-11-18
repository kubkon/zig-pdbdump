const StringTable = @This();

const std = @import("std");
const assert = std.debug.assert;
const hash_functions = @import("hash.zig");
const log = std.log;
const mem = std.mem;

const Allocator = mem.Allocator;
const HashSet = @import("hash_set.zig").HashSet;

allocator: Allocator,
hash_version: u32,
bytes: std.ArrayListUnmanaged(u8) = .{},
lookup: HashSet(IndexContext) = .{},

const IndexContext = struct {
    bytes: []const u8,
    hash_version: u32,

    pub fn hash(ctx: @This(), key: u32) u32 {
        const slice = mem.sliceTo(@ptrCast([*:0]const u8, ctx.bytes.ptr) + key, 0);
        switch (ctx.hash_version) {
            1 => return @truncate(u16, hash_functions.hashStringV1(slice)),
            2 => return hash_functions.hashStringV2(slice),
            else => unreachable, // unsupported hash version
        }
    }

    pub fn eql(ctx: @This(), key1: u32, key2: u32) bool {
        _ = ctx;
        return key1 == key2;
    }
};

const IndexAdapter = struct {
    bytes: []const u8,
    hash_version: u32,

    pub fn hash(ctx: @This(), key: []const u8) u32 {
        switch (ctx.hash_version) {
            1 => return @truncate(u16, hash_functions.hashStringV1(key)),
            2 => return hash_functions.hashStringV2(key),
            else => unreachable, // unsupported hash version
        }
    }

    pub fn eql(ctx: @This(), key1: []const u8, key2: u32) bool {
        const slice = mem.sliceTo(@ptrCast([*:0]const u8, ctx.bytes.ptr) + key2, 0);
        return mem.eql(u8, key1, slice);
    }
};

pub fn deinit(self: *StringTable, allocator: Allocator) void {
    self.bytes.deinit(allocator);
    self.lookup.deinit(allocator);
}

pub fn read(allocator: Allocator, hash_version: u32, bytes_size: u32, reader: anytype) !StringTable {
    var self = StringTable{ .allocator = allocator, .hash_version = hash_version };

    try self.bytes.resize(allocator, bytes_size);
    const amt = try reader.readAll(self.bytes.items);
    if (amt != bytes_size) return error.InputOutput;

    self.lookup = try HashSet(IndexContext).read(allocator, reader);

    return self;
}

pub fn getString(self: StringTable, off: u32) []const u8 {
    assert(off < self.bytes.items.len);
    return mem.sliceTo(@ptrCast([*:0]const u8, self.bytes.items.ptr) + off, 0);
}

pub fn getOffset(self: StringTable, name: []const u8) ?u32 {
    return self.lookup.get(name, IndexAdapter{
        .bytes = self.bytes.items,
        .hash_version = self.hash_version,
    });
}
