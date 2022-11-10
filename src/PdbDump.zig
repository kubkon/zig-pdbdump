const PdbDump = @This();

const std = @import("std");
const fs = std.fs;
const io = std.io;
const log = std.log;
const mem = std.mem;
const pdb = std.pdb;

const Allocator = mem.Allocator;

gpa: Allocator,
data: []const u8,

pub fn parse(gpa: Allocator, file: fs.File) !PdbDump {
    const file_size = try file.getEndPos();
    const data = try file.readToEndAlloc(gpa, file_size);
    errdefer gpa.free(data);

    var self = PdbDump{
        .gpa = gpa,
        .data = data,
    };

    return self;
}

pub fn deinit(self: *PdbDump) void {
    self.gpa.free(self.data);
}

pub fn printHeaders(self: PdbDump, writer: anytype) !void {
    const super_block = @ptrCast(*align(1) const pdb.SuperBlock, self.data[0..@sizeOf(pdb.SuperBlock)]).*;

    if (!mem.eql(u8, &super_block.FileMagic, pdb.SuperBlock.file_magic)) {
        log.err("invalid PDB magic: expected {s} but got {s}", .{
            pdb.SuperBlock.file_magic,
            super_block.FileMagic,
        });
        return error.InvalidPdb;
    }

    inline for (@typeInfo(pdb.SuperBlock).Struct.fields) |field| {
        try writer.print("{s: <26} ", .{field.name});
        if (comptime mem.eql(u8, "FileMagic", field.name)) {
            try writer.print("{s}", .{std.fmt.fmtSliceEscapeLower(&@field(super_block, field.name))});
        } else {
            try writer.print("{x}", .{@field(super_block, field.name)});
        }
        try writer.writeByte('\n');
    }

    var count: usize = 1;
    while (count < super_block.NumBlocks) : (count += 1) {
        const block = self.data[count * super_block.BlockSize ..][0..super_block.BlockSize];
        _ = block;
    }
}
