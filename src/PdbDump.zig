const PdbDump = @This();

const std = @import("std");
const fs = std.fs;
const io = std.io;
const log = std.log;
const mem = std.mem;

const Allocator = mem.Allocator;

const pdb = @import("pdb.zig");

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
    // TODO triggers no struct layout assert
    // if (self.data.len < @sizeOf(pdb.SuperBlock)) {
    if (self.data.len < 56) {
        log.err("file too short", .{});
        return error.InvalidPdb;
    }

    const super_block = self.getMsfSuperBlock();

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

    try writer.writeByte('\n');

    var fb_map_it = self.getMsfFreeBlockMapIterator();
    while (fb_map_it.next()) |block| {
        const index = (fb_map_it.count - 1) * super_block.BlockSize + super_block.FreeBlockMapBlock;
        try writer.print("FreeBlockMap #0x{x}\n", .{index});

        var state: enum { free, taken } = if (@truncate(u1, block[0]) == 1) .free else .taken;
        var start_block_index: u32 = 0;
        var bit_count: u32 = 0;
        const total_block_count: u32 = 8 * super_block.BlockSize;
        while (bit_count < total_block_count) : (bit_count += 1) {
            const block_index = bit_count;
            const byte_index = @divTrunc(bit_count, 8);
            const shift = @intCast(u3, @mod(bit_count, 8));
            const free = @truncate(u1, block[byte_index] >> shift) == 1;

            switch (state) {
                .free => if (!free) {
                    try writer.print("  #{x: >4} - {x: >4} free\n", .{ start_block_index, block_index - 1 });
                    start_block_index = block_index;
                    state = .taken;
                },
                .taken => if (free) {
                    try writer.print("  #{x: >4} - {x: >4} taken\n", .{ start_block_index, block_index - 1 });
                    start_block_index = block_index;
                    state = .free;
                },
            }
        }

        if (start_block_index < total_block_count - 1) {
            try writer.print("  #{x: >4} - {x: >4} {s}\n", .{
                start_block_index, total_block_count - 1,
                switch (state) {
                    .free => "free",
                    .taken => "taken",
                },
            });
        }
    }

    try writer.writeByte('\n');

    const stream_dir = try self.getStreamDirectory();
    try writer.writeAll("StreamDirectory\n");

    const num_streams = try stream_dir.getNumStreams();
    try writer.print("  {s: <16} {x}\n", .{ "NumStreams", num_streams });

    const stream_sizes = try stream_dir.getStreamSizes();
    try writer.print("  {s: <16} ", .{"StreamSizes"});
    for (stream_sizes) |size| {
        try writer.print("{x} ", .{size});
    }
    try writer.writeByte('\n');

    try writer.print("  {s: <16} ", .{"StreamBlocks"});
    var i: usize = 0;
    while (i < num_streams) : (i += 1) {
        const stream = try stream_dir.getMsfStreamAt(i);
        try writer.writeByte('\n');
        try writer.print("    #{x}: ", .{i});
        for (stream.blocks) |block| {
            try writer.print("{x} ", .{block});
        }
    }
    try writer.writeByte('\n');
    try writer.writeByte('\n');

    if (stream_dir.getMsfStreamAt(1)) |pdb_stream| {
        try writer.writeAll("PDB Info Stream #1\n");
        // PDBStream is at index #1
        const header = try pdb_stream.read(pdb.PdbStreamHeader, 0);

        inline for (@typeInfo(pdb.PdbStreamHeader).Struct.fields) |field| {
            try writer.print("  {s: <16} ", .{field.name});

            const value = @field(header, field.name);

            if (comptime mem.eql(u8, field.name, "Version")) {
                try writer.print("{s}", .{@tagName(value)});
            } else if (comptime mem.eql(u8, field.name, "Guid")) {
                try writer.print("{x}", .{std.fmt.fmtSliceHexLower(&value)});
            } else {
                try writer.print("{x}", .{value});
            }

            try writer.writeByte('\n');
        }
    } else |err| switch (err) {
        error.EndOfStream => try writer.writeAll("No PDB Info Stream found.\n"),
        else => |e| return e,
    }
}

fn getMsfSuperBlock(self: *const PdbDump) *align(1) const pdb.SuperBlock {
    return @ptrCast(*align(1) const pdb.SuperBlock, self.data.ptr);
}

const Block = []const u8;

const FreeBlockMapIterator = struct {
    self: *const PdbDump,
    count: usize = 0,

    fn next(it: *FreeBlockMapIterator) ?Block {
        const super_block = it.self.getMsfSuperBlock();
        const index = it.count * super_block.BlockSize + super_block.FreeBlockMapBlock;
        if (index >= super_block.NumBlocks) return null;
        it.count += 1;
        return it.self.data[index * super_block.BlockSize ..][0..super_block.BlockSize];
    }
};

fn getMsfFreeBlockMapIterator(self: *const PdbDump) FreeBlockMapIterator {
    return .{ .self = self };
}

const StreamDirectory = struct {
    stream: MsfStream,

    const invalid_stream: u32 = @bitCast(u32, @as(i32, -1));

    fn getNumStreams(dir: StreamDirectory) !u32 {
        return dir.stream.read(u32, 0);
    }

    fn getStreamSizes(dir: StreamDirectory) ![]align(1) const u32 {
        const num_streams = try dir.getNumStreams();
        const raw_stream_sizes = try dir.stream.bytes(@sizeOf(u32), @sizeOf(u32) * num_streams);
        return @ptrCast([*]align(1) const u32, raw_stream_sizes.ptr)[0..num_streams];
    }

    fn getMsfStreamAt(dir: StreamDirectory, index: usize) !MsfStream {
        const num_streams = try dir.getNumStreams();
        if (index >= num_streams) return error.EndOfStream;

        const block_size = dir.stream.ctx.getMsfSuperBlock().BlockSize;
        const stream_sizes = try dir.getStreamSizes();
        var stream_size = stream_sizes[index];
        if (stream_size == invalid_stream) stream_size = 0;
        const num_blocks = ceil(u32, stream_size, block_size);

        const total_prev_num_blocks = blk: {
            var sum: u32 = 0;
            var i: usize = 0;
            while (i < index) : (i += 1) {
                var prev_size = stream_sizes[i];
                if (prev_size == invalid_stream) prev_size = 0;
                sum += ceil(u32, prev_size, block_size);
            }
            break :blk sum;
        };

        const pos = @sizeOf(u32) * (stream_sizes.len + 1 + total_prev_num_blocks);
        const raw_bytes = try dir.stream.bytes(pos, num_blocks * @sizeOf(u32));
        const blocks = @ptrCast([*]align(1) const u32, raw_bytes.ptr)[0..num_blocks];

        return MsfStream{ .ctx = dir.stream.ctx, .blocks = blocks };
    }
};

inline fn ceil(comptime T: type, num: T, div: T) T {
    return @divTrunc(num, div) + @boolToInt(@rem(num, div) > 0);
}

fn getStreamDirectory(self: *const PdbDump) !StreamDirectory {
    const super_block = self.getMsfSuperBlock();
    const pos = super_block.BlockMapAddr * super_block.BlockSize;
    const num = ceil(u32, super_block.NumDirectoryBytes, super_block.BlockSize);
    const blocks = @ptrCast([*]align(1) const u32, self.data.ptr + pos)[0..num];
    const stream = MsfStream{ .ctx = self, .blocks = blocks };
    return StreamDirectory{ .stream = stream };
}

const MsfStream = struct {
    ctx: *const PdbDump,
    blocks: []align(1) const u32,

    const max_block_size: u32 = 0x1000;

    fn bytes(stream: MsfStream, pos: usize, len: usize) ![]const u8 {
        const super_block = stream.ctx.getMsfSuperBlock();

        if (pos + len > stream.blocks.len * super_block.BlockSize) {
            return error.EndOfStream;
        }

        if (len <= 2 * max_block_size) {

            // Hone in on the block(s) containing T and stitch together
            // two subsequent blocks.
            const index = @divTrunc(pos, super_block.BlockSize);
            var buffer: [2 * max_block_size]u8 = undefined;
            {
                const abs_pos = stream.blocks[index] * super_block.BlockSize;
                mem.copy(u8, &buffer, stream.ctx.data[abs_pos..][0..super_block.BlockSize]);
            }
            if (index < stream.blocks.len - 1) {
                const abs_pos = stream.blocks[index + 1] * super_block.BlockSize;
                mem.copy(
                    u8,
                    buffer[super_block.BlockSize..],
                    stream.ctx.data[abs_pos..][0..super_block.BlockSize],
                );
            }
            const rel_pos = @rem(pos, super_block.BlockSize);

            return buffer[rel_pos..][0..len];
        } else {
            log.err("TODO stitching of very large blocks", .{});
            return error.TODOLargeBlocks;
        }
    }

    fn read(stream: MsfStream, comptime T: type, pos: usize) !T {
        switch (@typeInfo(T)) {
            .Int => {
                const raw_bytes = try stream.bytes(pos, @sizeOf(T));
                return mem.readIntLittle(T, raw_bytes[0..@sizeOf(T)]);
            },
            .Struct => {
                const raw_bytes = try stream.bytes(pos, @sizeOf(T));
                return @ptrCast(*align(1) const T, raw_bytes).*;
            },
            else => @compileError("TODO unhandled"),
        }
    }
};
