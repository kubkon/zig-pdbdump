const PdbDump = @This();

const std = @import("std");
const assert = std.debug.assert;
const hash_table = @import("hash_table.zig");
const fs = std.fs;
const io = std.io;
const log = std.log;
const mem = std.mem;
const pdb = @import("pdb.zig");

const Allocator = mem.Allocator;
const HashTable = hash_table.HashTable;

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
    try writer.writeAll("StreamDirectory\n");

    const stream_dir = try self.getStreamDirectory();
    defer stream_dir.deinit(self.gpa);

    const num_streams = stream_dir.getNumStreams();
    const stream_sizes = stream_dir.getStreamSizes();

    try writer.print("  {s: <16} {x}\n", .{ "NumStreams", num_streams });
    try writer.print("  {s: <16} ", .{"StreamSizes"});
    for (stream_sizes) |size| {
        try writer.print("{x} ", .{size});
    }
    try writer.writeByte('\n');
    try writer.print("  {s: <16} ", .{"StreamBlocks"});

    var i: usize = 0;
    while (i < num_streams) : (i += 1) {
        try writer.writeByte('\n');
        try writer.print("    #{x}: ", .{i});
        const blocks = stream_dir.getStreamBlocks(i, .{
            .data = self.data,
            .block_size = super_block.BlockSize,
        }).?;
        for (blocks) |block| {
            try writer.print("{x} ", .{block});
        }
    }
    try writer.writeByte('\n');
    try writer.writeByte('\n');

    if (try stream_dir.streamAtAlloc(self.gpa, 1, .{
        .data = self.data,
        .block_size = super_block.BlockSize,
    })) |pdb_stream| {
        defer self.gpa.free(pdb_stream);

        var stream = std.io.fixedBufferStream(pdb_stream);
        var creader = std.io.countingReader(stream.reader());
        const reader = creader.reader();

        try writer.writeAll("PDB Info Stream #1\n");
        // PDBStream is at index #1
        const header = try reader.readStruct(pdb.PdbStreamHeader);

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

        var named_stream_map = try NamedStreamMap.read(self.gpa, reader);
        defer named_stream_map.deinit(self.gpa);

        var nsm_it = named_stream_map.iterator();
        while (nsm_it.next()) |name| {
            const stream_index = named_stream_map.getStreamIndex(name).?;
            log.warn("stream '{s}' at index #{x}", .{ name, stream_index });
        }

        if (named_stream_map.getStreamIndex("/TMCache")) |stream_index| {
            log.warn("stream '/TMCache' at index #{x}", .{stream_index});
        } else {
            log.warn("stream '/TMCache' not found", .{});
        }

        const num_features = @divExact(pdb_stream.len - creader.bytes_read, @sizeOf(pdb.PdbFeatureCode));
        const features = try self.gpa.alloc(pdb.PdbFeatureCode, num_features);
        defer self.gpa.free(features);
        _ = try reader.readAll(@ptrCast([*]u8, features.ptr)[0 .. num_features * @sizeOf(pdb.PdbFeatureCode)]);

        for (features) |feature| {
            log.warn("feature = {}", .{feature});
        }
    } else {
        try writer.writeAll("No PDB Info Stream found.\n");
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

const Ctx = struct {
    data: []align(1) const u8,
    block_size: u32,
};

const StreamDirectory = struct {
    stream: MsfStream,

    const invalid_stream: u32 = @bitCast(u32, @as(i32, -1));

    fn deinit(dir: StreamDirectory, gpa: Allocator) void {
        gpa.free(dir.stream);
    }

    fn getNumStreams(dir: StreamDirectory) u32 {
        return @ptrCast(*align(1) const u32, dir.stream.ptr).*;
    }

    fn getStreamSizes(dir: StreamDirectory) []align(1) const u32 {
        const num_streams = dir.getNumStreams();
        return @ptrCast([*]align(1) const u32, dir.stream.ptr + @sizeOf(u32))[0..num_streams];
    }

    fn getStreamBlocks(dir: StreamDirectory, index: usize, ctx: Ctx) ?[]align(1) const u32 {
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

    fn streamAtAlloc(dir: StreamDirectory, gpa: Allocator, index: usize, ctx: Ctx) error{OutOfMemory}!?MsfStream {
        const blocks = dir.getStreamBlocks(index, ctx) orelse return null;
        const size = dir.getStreamSizes()[index];

        const buffer = try gpa.alloc(u8, size);

        stitchBlocks(blocks, buffer, ctx);

        return buffer;
    }
};

inline fn ceil(comptime T: type, num: T, div: T) T {
    return @divTrunc(num, div) + @boolToInt(@rem(num, div) > 0);
}

fn getStreamDirectory(self: *const PdbDump) error{OutOfMemory}!StreamDirectory {
    const super_block = self.getMsfSuperBlock();
    const pos = super_block.BlockMapAddr * super_block.BlockSize;
    const num = ceil(u32, super_block.NumDirectoryBytes, super_block.BlockSize);
    const blocks = @ptrCast([*]align(1) const u32, self.data.ptr + pos)[0..num];

    const buffer = try self.gpa.alloc(u8, super_block.NumDirectoryBytes);

    stitchBlocks(blocks, buffer, .{
        .data = self.data,
        .block_size = super_block.BlockSize,
    });

    return StreamDirectory{ .stream = buffer };
}

pub const MsfStream = []const u8;

fn stitchBlocks(blocks: []align(1) const u32, buffer: []u8, ctx: Ctx) void {
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

const NamedStreamMap = struct {
    strtab: std.ArrayListUnmanaged(u8) = .{},
    hash_table: HashTable(u32) = .{},

    fn deinit(self: *NamedStreamMap, gpa: Allocator) void {
        self.strtab.deinit(gpa);
        self.hash_table.deinit(gpa);
    }

    pub const HashContext = struct {
        map: *const NamedStreamMap,

        pub fn hash(ctx: @This(), key: []const u8) u32 {
            _ = ctx;
            // It is a bug not to truncate a valid u32 to u16.
            return @truncate(u16, hash_table.hashStringV1(key));
        }

        pub fn invHash(ctx: @This(), offset: u32) ?[]const u8 {
            if (offset > ctx.map.strtab.items.len) return null;
            return mem.sliceTo(@ptrCast([*:0]u8, ctx.map.strtab.items.ptr + offset), 0);
        }
    };

    fn getStreamIndex(self: NamedStreamMap, key: []const u8) ?u32 {
        const res = self.hash_table.getIndexOrFirstUnused([]const u8, HashContext, key, .{ .map = &self });
        if (!res.existing) {
            return null;
        }
        return self.hash_table.buckets.items[res.index].value;
    }

    fn read(gpa: Allocator, reader: anytype) !NamedStreamMap {
        var map = NamedStreamMap{};

        const strtab_len = try reader.readIntLittle(u32);
        try map.strtab.resize(gpa, strtab_len);
        const amt = try reader.readAll(map.strtab.items);
        if (amt != strtab_len) return error.InputOutput;

        map.hash_table = try HashTable(u32).read(gpa, reader);

        return map;
    }

    const Iterator = struct {
        strtab: []const u8,
        pos: usize = 0,

        fn next(it: *Iterator) ?[]const u8 {
            if (it.pos == it.strtab.len) return null;
            const str = mem.sliceTo(@ptrCast([*:0]const u8, it.strtab.ptr + it.pos), 0);
            it.pos += str.len + 1;
            return str;
        }
    };

    fn iterator(self: NamedStreamMap) Iterator {
        return .{ .strtab = self.strtab.items };
    }
};
