//! Named Stream Map
const NamedStreamMap = @This();

const std = @import("std");
const assert = std.debug.assert;
const hashStringV1 = @import("hash.zig").hashStringV1;
const mem = std.mem;

const Allocator = mem.Allocator;
const HashTable = @import("hash_table.zig").HashTable;

gpa: Allocator,
strtab: std.ArrayListUnmanaged(u8) = .{},
hash_table: HashTable(u32, IndexContext) = .{},

const IndexContext = struct {
    strtab: []const u8,

    pub fn hash(ctx: @This(), key: u32) u32 {
        const slice = mem.sliceTo(@ptrCast([*:0]const u8, ctx.strtab.ptr) + key, 0);
        // It is a bug not to truncate a valid u32 to u16.
        return @truncate(u16, hashStringV1(slice));
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
        return @truncate(u16, hashStringV1(key));
    }

    pub fn eql(ctx: @This(), key1: []const u8, key2: u32) bool {
        const slice = mem.sliceTo(@ptrCast([*:0]const u8, ctx.strtab.ptr) + key2, 0);
        return mem.eql(u8, key1, slice);
    }
};

pub fn deinit(self: *NamedStreamMap) void {
    self.strtab.deinit(self.gpa);
    self.hash_table.deinit(self.gpa);
}

pub fn get(self: NamedStreamMap, key: []const u8) ?u32 {
    return self.hash_table.get(key, IndexAdapter{ .strtab = self.strtab.items });
}

pub fn put(self: *NamedStreamMap, key: []const u8, value: u32) !void {
    const gop = try self.hash_table.getOrPut(
        key,
        IndexAdapter{ .strtab = self.strtab.items },
        IndexContext{ .strtab = self.strtab.items },
    );
    if (gop.found_existing) {
        gop.value_ptr.* = value;
        return;
    }

    const offset = @intCast(u32, self.strtab.items.len);
    try self.strtab.ensureUnusedCapacity(self.gpa, key.len + 1);
    self.strtab.appendSliceAssumeCapacity(key);
    self.strtab.appendAssumeCapacity(0);

    gop.key_ptr.* = offset;
    gop.value_ptr.* = value;
}

pub fn getString(self: NamedStreamMap, off: u32) []const u8 {
    assert(off < self.strtab.items.len);
    return mem.sliceTo(@ptrCast([*:0]const u8, self.strtab.items.ptr) + off, 0);
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

    map.hash_table = try HashTable(u32, IndexContext).read(gpa, reader);

    return map;
}

pub fn write(self: NamedStreamMap, writer: anytype) !void {
    try writer.writeIntLittle(u32, @intCast(u32, self.strtab.items.len));
    try writer.writeAll(self.strtab.items);
    try self.hash_table.write(writer);
}
