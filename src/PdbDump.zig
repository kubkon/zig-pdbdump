const PdbDump = @This();

const std = @import("std");
const fs = std.fs;
const mem = std.mem;

const Allocator = mem.Allocator;

gpa: Allocator,

pub fn parse(gpa: Allocator, file: fs.File) !PdbDump {
    _ = file;
    return PdbDump{ .gpa = gpa };
}

pub fn deinit(self: *PdbDump) void {
    _ = self;
}

pub fn printHeaders(self: PdbDump, writer: anytype) !void {
    _ = self;
    _ = writer;
}
