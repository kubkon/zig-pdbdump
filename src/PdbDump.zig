const PdbDump = @This();

const std = @import("std");
const fs = std.fs;
const io = std.io;
const log = std.log;
const mem = std.mem;
const pdb = @import("pdb.zig");

const Allocator = mem.Allocator;
const BitSet = @import("BitSet.zig");

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

pub fn printHeaders(self: *const PdbDump, writer: anytype) !void {
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
    defer stream_dir.deinit(self.gpa);

    try writer.writeAll("StreamDirectory\n");

    const num_streams = stream_dir.getNumStreams();
    try writer.print("  {s: <16} {x}\n", .{ "NumStreams", num_streams });

    const stream_sizes = stream_dir.getStreamSizes();
    try writer.print("  {s: <16} ", .{"StreamSizes"});
    for (stream_sizes) |size| {
        try writer.print("{x} ", .{size});
    }
    try writer.writeByte('\n');

    try writer.print("  {s: <16} ", .{"StreamBlocks"});

    var i: usize = 0;
    while (i < num_streams) : (i += 1) {
        const stream = try stream_dir.getMsfStreamAt(self.gpa, i);
        defer stream.deinit(self.gpa);

        try writer.writeByte('\n');
        try writer.print("    #{x}: ", .{i});
        for (stream.blocks()) |block| {
            try writer.print("{x} ", .{block});
        }
    }
    try writer.writeByte('\n');
    try writer.writeByte('\n');

    if (stream_dir.getMsfStreamAt(self.gpa, 1)) |pdb_stream| {
        defer pdb_stream.deinit(self.gpa);

        try writer.writeAll("PDB Info Stream #1\n");
        // PDBStream is at index #1
        var pos: usize = 0;
        const all_bytes = try pdb_stream.bytesAlloc(self.gpa, 0, pdb_stream.size);
        log.warn("{x}", .{std.fmt.fmtSliceHexLower(all_bytes)});

        const header = try pdb_stream.read(pdb.PdbStreamHeader, pos);
        pos += @sizeOf(pdb.PdbStreamHeader);
        log.warn("pos = {x}", .{pos});

        inline for (@typeInfo(pdb.PdbStreamHeader).Struct.fields) |field| {
            try writer.print("  {s: <16} ", .{field.name});

            const value = @field(header, field.name);

            if (comptime mem.eql(u8, field.name, "Version")) {
                try writer.print("{s}", .{@tagName(value)});
            } else if (comptime mem.eql(u8, field.name, "Guid")) {
                try writer.print("{x}", .{std.fmt.fmtSliceHexLower(&value)});
            } else {
                try writer.print("{d}", .{value});
            }

            try writer.writeByte('\n');
        }

        const strtab_len = try pdb_stream.read(u32, pos);
        pos += @sizeOf(u32);

        const strtab = try pdb_stream.bytesAlloc(self.gpa, pos, strtab_len);
        defer self.gpa.free(strtab);
        pos += strtab_len;
        log.warn("{s}", .{std.fmt.fmtSliceEscapeLower(strtab)});

        const map_len = try pdb_stream.read(u32, pos);
        pos += @sizeOf(u32);
        log.warn("size = {x}", .{map_len});

        const map_capacity = try pdb_stream.read(u32, pos);
        pos += @sizeOf(u32);
        log.warn("capacity = {x}", .{map_capacity});

        const num_masks = try pdb_stream.read(u32, pos);
        pos += @sizeOf(u32);

        var present_bit_set = try readBitSet(pdb_stream, self.gpa, pos, num_masks);
        defer present_bit_set.deinit(self.gpa);
        pos += BitSet.numMasks(present_bit_set.bit_length) * @sizeOf(u32);

        var deleted_bit_set = try readBitSet(pdb_stream, self.gpa, pos, num_masks);
        defer deleted_bit_set.deinit(self.gpa);
        pos += BitSet.numMasks(deleted_bit_set.bit_length) * @sizeOf(u32);

        {
            var bucket_i: usize = 0;
            while (bucket_i < map_capacity) : (bucket_i += 1) {
                if (present_bit_set.isSet(bucket_i)) {
                    log.warn("  slot {x} used", .{bucket_i});
                } else if (deleted_bit_set.capacity() > 0 and deleted_bit_set.isSet(bucket_i)) {
                    log.warn("  slot {x} tombstone", .{bucket_i});
                } else {
                    log.warn("  slot {x} empty", .{bucket_i});
                }
            }
        }

        log.warn("pos = {x}, size = {x}", .{ pos, pdb_stream.size });
        var bucket_count: usize = 0;
        while (bucket_count < map_capacity) : (bucket_count += 1) {
            const key = try pdb_stream.read(u32, pos);
            const value = try pdb_stream.read(u32, pos + @sizeOf(u32));
            pos += 2 * @sizeOf(u32);

            log.warn("  key = {x}, value = {x}", .{ key, value });
        }

        log.warn("pos = {x}, size = {x}", .{ pos, pdb_stream.size });
        const features_raw = try pdb_stream.bytesAlloc(self.gpa, pos, pdb_stream.size - pos);
        defer self.gpa.free(features_raw);
        const num_features = @divExact(features_raw.len, @sizeOf(u32));
        const features = @ptrCast([*]align(1) const u32, features_raw.ptr)[0..num_features];

        for (features) |feature| {
            log.warn("feature = {x}", .{feature});
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
    sizes: []const u8,
    stream: MsfStream,

    const invalid_stream: u32 = @bitCast(u32, @as(i32, -1));

    fn deinit(dir: StreamDirectory, gpa: Allocator) void {
        gpa.free(dir.sizes);
    }

    fn getNumStreams(dir: StreamDirectory) usize {
        return @divExact(dir.sizes.len, @sizeOf(u32));
    }

    fn getStreamSizes(dir: StreamDirectory) []align(1) const u32 {
        const num_streams = dir.getNumStreams();
        return @ptrCast([*]align(1) const u32, dir.sizes.ptr)[0..num_streams];
    }

    fn getMsfStreamAt(dir: StreamDirectory, gpa: Allocator, index: usize) !MsfStream {
        const num_streams = dir.getNumStreams();
        if (index >= num_streams) return error.EndOfStream;

        const block_size = dir.stream.ctx.getMsfSuperBlock().BlockSize;
        const stream_sizes = dir.getStreamSizes();
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
        const buffer = try dir.stream.bytesAlloc(gpa, pos, num_blocks * @sizeOf(u32));

        return MsfStream{ .ctx = dir.stream.ctx, .buffer = buffer, .size = stream_size };
    }
};

inline fn ceil(comptime T: type, num: T, div: T) T {
    return @divTrunc(num, div) + @boolToInt(@rem(num, div) > 0);
}

fn getStreamDirectory(self: *const PdbDump) !StreamDirectory {
    const super_block = self.getMsfSuperBlock();
    const pos = super_block.BlockMapAddr * super_block.BlockSize;
    const stream = MsfStream{
        .ctx = self,
        .buffer = self.data[pos..][0..super_block.NumDirectoryBytes],
        .size = super_block.NumDirectoryBytes,
    };
    const num_streams = try stream.read(u32, 0);
    const sizes = try stream.bytesAlloc(self.gpa, @sizeOf(u32), num_streams * @sizeOf(u32));

    return StreamDirectory{ .sizes = sizes, .stream = stream };
}

const MsfStream = struct {
    ctx: *const PdbDump,
    buffer: []const u8,
    size: usize,

    fn deinit(stream: MsfStream, gpa: Allocator) void {
        gpa.free(stream.buffer);
    }

    fn blocks(stream: MsfStream) []align(1) const u32 {
        const num = @divExact(stream.buffer.len, @sizeOf(u32));
        return @ptrCast([*]align(1) const u32, stream.buffer.ptr)[0..num];
    }

    fn bytesAlloc(stream: MsfStream, gpa: Allocator, pos: usize, len: usize) ![]const u8 {
        const buffer = try gpa.alloc(u8, len);
        return stream.bytes(pos, buffer);
    }

    fn bytes(stream: MsfStream, pos: usize, buffer: []u8) ![]const u8 {
        if (pos + buffer.len > stream.size) return error.EndOfStream;

        const super_block = stream.ctx.getMsfSuperBlock();
        const stream_blocks = stream.blocks();

        // Hone in on the block(s) containing T and stitch together
        // N subsequent blocks.
        var out = buffer;
        var index = @divTrunc(pos, super_block.BlockSize);
        var init_pos = @rem(pos, super_block.BlockSize) + stream_blocks[index] * super_block.BlockSize;
        const init_len = @min(buffer.len, super_block.BlockSize - @rem(pos, super_block.BlockSize));
        mem.copy(u8, out, stream.ctx.data[init_pos..][0..init_len]);
        out = out[init_len..];

        var leftover = buffer.len - init_len;
        while (leftover > 0) {
            index += 1;
            const next_pos = stream_blocks[index] * super_block.BlockSize;
            const copy_len = @min(leftover, super_block.BlockSize);
            mem.copy(u8, out, stream.ctx.data[next_pos..][0..copy_len]);
            out = out[copy_len..];
            leftover -= copy_len;
        }

        return buffer;
    }

    fn read(stream: MsfStream, comptime T: type, pos: usize) !T {
        var buffer: [@sizeOf(T)]u8 = undefined;
        const raw_bytes = try stream.bytes(pos, &buffer);
        return switch (@typeInfo(T)) {
            .Int => mem.readIntLittle(T, raw_bytes[0..@sizeOf(T)]),
            .Struct => @ptrCast(*align(1) const T, raw_bytes).*,
            else => @compileError("TODO unhandled"),
        };
    }
};

fn readBitSet(stream: MsfStream, gpa: Allocator, pos: usize, num_masks: u32) !BitSet {
    const raw_bytes = try stream.bytesAlloc(gpa, pos, num_masks * @sizeOf((u32)));
    defer gpa.free(raw_bytes);

    const bit_length = num_masks * @bitSizeOf(u32);
    var bit_set = try BitSet.initEmpty(gpa, bit_length);

    const masks = @ptrCast([*]align(1) const u32, raw_bytes.ptr)[0..num_masks];
    for (masks) |mask, i| {
        log.warn("mask {x}", .{mask});
        bit_set.masks[i] = mask;
    }

    return bit_set;
}
