const std = @import("std");
const clap = @import("clap");
const process = std.process;

const PdbDump = @import("PdbDump.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub fn main() !void {
    const stderr = std.io.getStdErr().writer();
    const stdout = std.io.getStdOut().writer();

    const params = comptime [_]clap.Param(clap.Help){
        clap.parseParam("--help                 Display this help and exit.") catch unreachable,
        clap.parseParam("--all                  Equivalent to specifying all flags.") catch unreachable,
        clap.parseParam("--msf-headers          Print MSF headers.") catch unreachable,
        clap.parseParam("--streams              Print stream directory.") catch unreachable,
        clap.parseParam("--info-stream          Print PDB Info Stream.") catch unreachable,
        clap.parseParam("<FILE>") catch unreachable,
    };

    const parsers = comptime .{
        .FILE = clap.parsers.string,
    };

    var res = try clap.parse(clap.Help, &params, parsers, .{
        .allocator = gpa.allocator(),
        .diagnostic = null,
    });
    defer res.deinit();

    if (res.args.help) {
        return printUsageWithHelp(stderr, params[0..]);
    }

    if (res.positionals.len == 0) {
        return stderr.print("missing positional argument <FILE>...\n", .{});
    }

    const filename = res.positionals[0];
    const file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var pdb = try PdbDump.parse(gpa.allocator(), file);
    defer pdb.deinit();

    if (res.args.@"msf-headers" or res.args.all) {
        try pdb.printMsfHeaders(stdout);
    }
    if (res.args.streams or res.args.all) {
        try pdb.printStreamDirectory(stdout);
    }
    if (res.args.@"info-stream" or res.args.all) {
        try pdb.printPdbInfoStream(stdout);
    }
}

fn printUsageWithHelp(stream: anytype, comptime params: []const clap.Param(clap.Help)) !void {
    try stream.print("pdbdump ", .{});
    try clap.usage(stream, clap.Help, params);
    try stream.print("\n", .{});
    try clap.help(stream, clap.Help, params, .{});
}

test {
    _ = @import("hash_table.zig");
}
