//! Stream Directory
const StreamDirectory = @This();

const std = @import("std");
const mem = std.mem;
const pdb = @import("pdb.zig");

const Allocator = mem.Allocator;

stream: MsfStream,

const invalid_stream: u32 = @bitCast(u32, @as(i32, -1));

pub const MsfStream = []const u8;

pub const Context = struct {
    data: []align(1) const u8,
    block_size: u32,
};

pub fn parse(
    gpa: Allocator,
    data: []const u8,
    super_block: *align(1) const pdb.SuperBlock,
) error{OutOfMemory}!StreamDirectory {
    const pos = super_block.BlockMapAddr * super_block.BlockSize;
    const num = ceil(u32, super_block.NumDirectoryBytes, super_block.BlockSize);
    const blocks = @ptrCast([*]align(1) const u32, data.ptr + pos)[0..num];

    const buffer = try gpa.alloc(u8, super_block.NumDirectoryBytes);

    stitchBlocks(blocks, buffer, .{
        .data = data,
        .block_size = super_block.BlockSize,
    });

    return StreamDirectory{ .stream = buffer };
}

pub fn deinit(dir: StreamDirectory, gpa: Allocator) void {
    gpa.free(dir.stream);
}

pub fn getNumStreams(dir: StreamDirectory) u32 {
    return @ptrCast(*align(1) const u32, dir.stream.ptr).*;
}

pub fn getStreamSizes(dir: StreamDirectory) []align(1) const u32 {
    const num_streams = dir.getNumStreams();
    return @ptrCast([*]align(1) const u32, dir.stream.ptr + @sizeOf(u32))[0..num_streams];
}

pub fn getStreamBlocks(dir: StreamDirectory, index: usize, ctx: Context) ?[]align(1) const u32 {
    const num_streams = dir.getNumStreams();
    if (index >= num_streams) return null;

    const stream_sizes = dir.getStreamSizes();
    const stream_size = stream_sizes[index];
    if (stream_size == invalid_stream or stream_size == 0) return &[0]u32{};

    const num_blocks = ceil(u32, stream_size, ctx.block_size);

    const total_prev_num_blocks = blk: {
        var sum: u32 = 0;
        var i: usize = 0;
        while (i < index) : (i += 1) {
            var prev_size = stream_sizes[i];
            if (prev_size == invalid_stream) prev_size = 0;
            sum += ceil(u32, prev_size, ctx.block_size);
        }
        break :blk sum;
    };

    const pos = @sizeOf(u32) * (stream_sizes.len + 1 + total_prev_num_blocks);
    return @ptrCast([*]align(1) const u32, dir.stream.ptr + pos)[0..num_blocks];
}

pub fn getStreamAlloc(dir: StreamDirectory, gpa: Allocator, index: usize, ctx: Context) error{OutOfMemory}!?MsfStream {
    const blocks = dir.getStreamBlocks(index, ctx) orelse return null;
    const size = dir.getStreamSizes()[index];

    const buffer = try gpa.alloc(u8, size);

    stitchBlocks(blocks, buffer, ctx);

    return buffer;
}

pub fn stitchBlocks(blocks: []align(1) const u32, buffer: []u8, ctx: Context) void {
    // Stitch together blocks belonging to the MsfStream.
    var out = buffer;
    var block_index: usize = 0;
    var init_pos = blocks[block_index] * ctx.block_size;
    const init_len = @min(buffer.len, ctx.block_size);
    mem.copy(u8, out, ctx.data[init_pos..][0..init_len]);
    out = out[init_len..];

    var leftover = buffer.len - init_len;
    while (leftover > 0) {
        block_index += 1;
        const next_pos = blocks[block_index] * ctx.block_size;
        const copy_len = @min(leftover, ctx.block_size);
        mem.copy(u8, out, ctx.data[next_pos..][0..copy_len]);
        out = out[copy_len..];
        leftover -= copy_len;
    }
}

inline fn ceil(comptime T: type, num: T, div: T) T {
    return @divTrunc(num, div) + @boolToInt(@rem(num, div) > 0);
}
