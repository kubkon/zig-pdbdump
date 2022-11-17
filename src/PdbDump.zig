const PdbDump = @This();

const std = @import("std");
const assert = std.debug.assert;
const fs = std.fs;
const io = std.io;
const log = std.log;
const mem = std.mem;
const pdb = @import("pdb.zig");

const Allocator = mem.Allocator;
const MsfStream = StreamDirectory.MsfStream;
const NamedStreamMap = @import("NamedStreamMap.zig");
const StreamDirectory = @import("StreamDirectory.zig");

gpa: Allocator,
data: []const u8,

stream_dir: StreamDirectory,
pdb_stream: ?PdbInfoStream = null,
pdb_strtab: ?PdbStringTableStream = null,

const PdbInfoStream = struct {
    stream: MsfStream,
    named_stream_map: NamedStreamMap,
    features_pos: u32,

    fn getHeader(self: *const @This()) *align(1) const pdb.PdbStreamHeader {
        return @ptrCast(*align(1) const pdb.PdbStreamHeader, self.stream.ptr);
    }

    fn getFeatures(self: @This()) []align(1) const pdb.PdbFeatureCode {
        const num_features = @divExact(self.stream.len - self.features_pos, @sizeOf(pdb.PdbFeatureCode));
        return @ptrCast([*]align(1) const pdb.PdbFeatureCode, self.stream.ptr + self.features_pos)[0..num_features];
    }
};

const PdbStringTableStream = struct {
    stream: MsfStream,

    fn getHeader(self: *const @This()) pdb.PDBStringTableHeader {
        return @ptrCast(*align(1) const pdb.PDBStringTableHeader, self.stream.ptr).*;
    }
};

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

    if (try self.stream_dir.getStreamAlloc(self.gpa, 1, .{
        .data = self.data,
        .block_size = self.getBlockSize(),
    })) |msf_stream| {
        var pdb_stream = PdbInfoStream{
            .stream = msf_stream,
            .named_stream_map = undefined,
            .features_pos = 0,
        };

        var stream = std.io.fixedBufferStream(msf_stream);
        var creader = std.io.countingReader(stream.reader());
        const reader = creader.reader();

        _ = try reader.readStruct(pdb.PdbStreamHeader);

        pdb_stream.named_stream_map = try NamedStreamMap.read(self.gpa, reader);
        pdb_stream.features_pos = @intCast(u32, creader.bytes_read);

        self.pdb_stream = pdb_stream;
    }

    if (self.pdb_stream) |pdb_stream| {
        if (pdb_stream.named_stream_map.get("/names")) |index| blk: {
            const msf_stream = (try self.stream_dir.getStreamAlloc(self.gpa, index, .{
                .data = self.data,
                .block_size = self.getBlockSize(),
            })) orelse break :blk;

            log.warn("{x}", .{std.fmt.fmtSliceEscapeLower(msf_stream)});

            var pdb_strtab = PdbStringTableStream{
                .stream = msf_stream,
            };

            log.warn("{}", .{pdb_strtab.getHeader()});
        }
    }

    return self;
}

pub fn deinit(self: *PdbDump) void {
    self.gpa.free(self.data);
    self.stream_dir.deinit(self.gpa);
    if (self.pdb_stream) |*stream| {
        self.gpa.free(stream.stream);
        stream.named_stream_map.deinit();
    }
    if (self.pdb_strtab) |*stream| {
        self.gpa.free(stream.stream);
    }
}

pub fn printMsfHeaders(self: *PdbDump, writer: anytype) !void {
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
}

pub fn printStreamDirectory(self: *const PdbDump, writer: anytype) !void {
    try writer.writeAll("Stream Directory\n");

    const num_streams = self.stream_dir.getNumStreams();
    const stream_sizes = self.stream_dir.getStreamSizes();

    var i: u32 = 0;
    while (i < num_streams) : (i += 1) {
        try writer.print("    Stream #{x: >3} ({x: >6} bytes): [{}]\n", .{
            i,
            stream_sizes[i],
            self.fmtStreamName(i),
        });
        const blocks = self.stream_dir.getStreamBlocks(i, .{
            .data = self.data,
            .block_size = self.getBlockSize(),
        }).?;

        try writer.writeAll("      Blocks:");
        for (blocks) |block, block_i| {
            if (block_i % 20 == 0) {
                try writer.writeAll("\n        ");
            }
            try writer.print("{x} ", .{block});
        }
        try writer.writeAll("\n\n");
    }
    try writer.writeByte('\n');
}

const PrintStreamNameArgs = struct {
    ctx: *const PdbDump,
    index: u32,
};

fn getAndPrintStreamName(
    val: PrintStreamNameArgs,
    comptime fmt: []const u8,
    options: std.fmt.FormatOptions,
    writer: anytype,
) !void {
    _ = options;
    comptime assert(fmt.len == 0);

    switch (val.index) {
        0 => return writer.writeAll("Old MSF Directory"),
        1 => return writer.writeAll("PDB Info Stream"),
        2 => return writer.writeAll("TPI Stream"),
        3 => return writer.writeAll("DBI Stream"),
        4 => return writer.writeAll("IPI Stream"),
        else => {},
    }

    if (val.ctx.pdb_stream) |pdb_stream| {
        var it = pdb_stream.named_stream_map.iterator();
        while (it.next()) |name| {
            if (pdb_stream.named_stream_map.get(name).? == val.index) {
                return writer.print("Named Stream \"{s}\"", .{name});
            }
        }
    }

    return writer.writeAll("???");
}

fn fmtStreamName(self: *const PdbDump, index: u32) std.fmt.Formatter(getAndPrintStreamName) {
    return .{ .data = .{
        .ctx = self,
        .index = index,
    } };
}

pub fn printPdbInfoStream(self: *const PdbDump, writer: anytype) !void {
    const pdb_stream = self.pdb_stream orelse {
        try writer.writeAll("No PDB Info Stream found.\n");
        return;
    };

    try writer.writeAll("PDB Info Stream #1\n");

    const header = pdb_stream.getHeader();

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

    const features = pdb_stream.getFeatures();

    try writer.print("  {s: <16} ", .{"Features"});
    for (features) |feature| {
        try writer.print("{}, ", .{feature});
    }
    try writer.writeByte('\n');

    try writer.writeAll("  Named Streams\n");

    var nsm_it = pdb_stream.named_stream_map.iterator();
    while (nsm_it.next()) |name| {
        const stream_index = pdb_stream.named_stream_map.get(name).?;
        try writer.print("    {s: <16}\n", .{name});
        try writer.print("      {s: <16} {x}\n", .{ "Index", stream_index });
        try writer.print("      {s: <16} {x}\n", .{ "Size (bytes)", self.stream_dir.getStreamSizes()[stream_index] });
    }

    try writer.writeByte('\n');
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
