//! Named Stream Map
const NamedStreamMap = @This();

const std = @import("std");
const hash_table = @import("hash_table.zig");
const mem = std.mem;

const Allocator = mem.Allocator;
const HashTable = hash_table.HashTable;

gpa: Allocator,
strtab: std.ArrayListUnmanaged(u8) = .{},
hash_table: HashTable(u32),

pub fn deinit(self: *NamedStreamMap) void {
    self.strtab.deinit(self.gpa);
    self.hash_table.deinit(self.gpa);
}

const HashContext = struct {
    map: *NamedStreamMap,

    pub fn hash(ctx: @This(), key: []const u8) u32 {
        _ = ctx;
        // It is a bug not to truncate a valid u32 to u16.
        return @truncate(u16, hash_table.hashStringV1(key));
    }

    pub fn eql(ctx: @This(), key1: []const u8, key2: []const u8) bool {
        _ = ctx;
        return mem.eql(u8, key1, key2);
    }

    pub fn getKeyAdapted(ctx: @This(), offset: u32) ?[]const u8 {
        if (offset > ctx.map.strtab.items.len) return null;
        return mem.sliceTo(@ptrCast([*:0]u8, ctx.map.strtab.items.ptr + offset), 0);
    }

    pub fn putKeyAdapted(ctx: @This(), key: []const u8) error{OutOfMemory}!u32 {
        const adapted = @intCast(u32, ctx.map.strtab.items.len);
        try ctx.map.strtab.ensureUnusedCapacity(ctx.map.gpa, key.len + 1);
        ctx.map.strtab.appendSliceAssumeCapacity(key);
        ctx.map.strtab.appendAssumeCapacity(0);
        return adapted;
    }
};

pub fn get(self: *NamedStreamMap, key: []const u8) ?u32 {
    return self.hash_table.get([]const u8, HashContext, key, .{ .map = self });
}

pub fn put(self: *NamedStreamMap, key: []const u8, value: u32) !void {
    try self.hash_table.put([]const u8, HashContext, key, value, .{ .map = self });
}

pub fn serializedSize(self: NamedStreamMap) u32 {
    return @intCast(u32, @sizeOf(u32) + self.strtab.items.len + self.hash_table.serializedSize());
}

pub fn read(gpa: Allocator, reader: anytype) !NamedStreamMap {
    var map = NamedStreamMap{
        .gpa = gpa,
        .hash_table = undefined,
    };

    const strtab_len = try reader.readIntLittle(u32);
    try map.strtab.resize(gpa, strtab_len);
    const amt = try reader.readAll(map.strtab.items);
    if (amt != strtab_len) return error.InputOutput;

    map.hash_table = try HashTable(u32).read(gpa, reader);

    return map;
}

pub fn write(self: NamedStreamMap, writer: anytype) !void {
    try writer.writeIntLittle(u32, @intCast(u32, self.strtab.items.len));
    try writer.writeAll(self.strtab.items);
    try self.hash_table.write(writer);
}

const Iterator = struct {
    strtab: []const u8,
    pos: usize = 0,

    pub fn next(it: *Iterator) ?[]const u8 {
        if (it.pos == it.strtab.len) return null;
        const str = mem.sliceTo(@ptrCast([*:0]const u8, it.strtab.ptr + it.pos), 0);
        it.pos += str.len + 1;
        return str;
    }
};

pub fn iterator(self: NamedStreamMap) Iterator {
    return .{ .strtab = self.strtab.items };
}
