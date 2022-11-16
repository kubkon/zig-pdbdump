const PdbDump = @This();

const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const io = std.io;
const log = std.log;
const mem = std.mem;
const pdb = @import("pdb.zig");

const Allocator = mem.Allocator;
const NamedStreamMap = @import("NamedStreamMap.zig");
const StreamDirectory = @import("StreamDirectory.zig");

gpa: Allocator,
data: []const u8,

stream_dir: StreamDirectory,
named_stream_map: ?NamedStreamMap = null,
strtab: ?[]const u8 = null,

pub fn parse(gpa: Allocator, file: fs.File) !PdbDump {
    const file_size = try file.getEndPos();
    const data = try file.readToEndAlloc(gpa, file_size);
    errdefer gpa.free(data);

    var self = PdbDump{
        .gpa = gpa,
        .data = data,
        .stream_dir = undefined,
    };

    self.stream_dir = try StreamDirectory.parse(self.gpa, self.data, self.getMsfSuperBlock());

    return self;
}

pub fn deinit(self: *PdbDump) void {
    self.gpa.free(self.data);
    self.stream_dir.deinit(self.gpa);
    if (self.named_stream_map) |*nsm| {
        nsm.deinit();
    }
    if (self.strtab) |strtab| {
        self.gpa.free(strtab);
    }
}

pub fn printHeaders(self: *PdbDump, writer: anytype) !void {
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

    const num_streams = self.stream_dir.getNumStreams();
    const stream_sizes = self.stream_dir.getStreamSizes();

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
        const blocks = self.stream_dir.getStreamBlocks(i, .{
            .data = self.data,
            .block_size = self.getBlockSize(),
        }).?;
        for (blocks) |block| {
            try writer.print("{x} ", .{block});
        }
    }
    try writer.writeByte('\n');
    try writer.writeByte('\n');

    if (try self.stream_dir.getStreamAlloc(self.gpa, 1, .{
        .data = self.data,
        .block_size = self.getBlockSize(),
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

        self.named_stream_map = try NamedStreamMap.read(self.gpa, reader);

        const num_features = @divExact(pdb_stream.len - creader.bytes_read, @sizeOf(pdb.PdbFeatureCode));
        const features = try self.gpa.alloc(pdb.PdbFeatureCode, num_features);
        defer self.gpa.free(features);
        _ = try reader.readAll(@ptrCast([*]u8, features.ptr)[0 .. num_features * @sizeOf(pdb.PdbFeatureCode)]);

        try writer.print("  {s: <16} ", .{"Features"});
        for (features) |feature| {
            try writer.print("{}, ", .{feature});
        }
        try writer.writeByte('\n');

        try writer.writeAll("  Named Streams\n");

        var nsm_it = self.named_stream_map.?.iterator();
        while (nsm_it.next()) |name| {
            const stream_index = self.named_stream_map.?.get(name).?;
            try writer.print("    {s: <16}\n", .{name});
            try writer.print("      {s: <16} {x}\n", .{ "Index", stream_index });
            try writer.print("      {s: <16} {x}\n", .{ "Size (bytes)", self.stream_dir.getStreamSizes()[stream_index] });
        }
    } else {
        try writer.writeAll("No PDB Info Stream found.\n");
    }

    if (self.getNamesStreamIndex()) |stream_index| blk: {
        const stream = (try self.stream_dir.getStreamAlloc(self.gpa, stream_index, .{
            .data = self.data,
            .block_size = self.getBlockSize(),
        })) orelse break :blk;
        defer self.gpa.free(stream);

        log.warn("{x}", .{std.fmt.fmtSliceEscapeLower(stream)});
    }
}

fn getMsfSuperBlock(self: *const PdbDump) *align(1) const pdb.SuperBlock {
    return @ptrCast(*align(1) const pdb.SuperBlock, self.data.ptr);
}

fn getBlockSize(self: PdbDump) u32 {
    return self.getMsfSuperBlock().BlockSize;
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

fn getNamesStreamIndex(self: *const PdbDump) ?u32 {
    const nsm = self.named_stream_map orelse return null;
    return nsm.get("/names");
}
