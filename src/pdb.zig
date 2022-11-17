const std = @import("std");
const pdb = std.pdb;

pub const SuperBlock = pdb.SuperBlock;
pub const PDBStringTableHeader = pdb.PDBStringTableHeader;

pub const PdbStreamHeader = extern struct {
    /// See `PdbStreamVersion` enum
    Version: PdbStreamVersion,

    /// A 32-bit time-stamp generated with a call to time()
    /// at the time the PDB file is written. Note that due
    /// to the inherent uniqueness problems of using a timestamp
    /// with 1-second granularity, this field does not really serve
    /// its intended purpose, and as such is typically ignored in
    /// favor of the Guid field, described below.
    Signature: u32,

    /// The number of times the PDB file has been written.
    /// This can be used along with Guid to match the PDB to its
    /// corresponding executable.
    Age: u32,

    /// A 128-bit identifier guaranteed to be unique across space and time.
    /// In general, this can be thought of as the result of calling the Win32 API
    /// UuidCreate, although LLVM cannot rely on that, as it must work on non-Windows
    /// platforms.
    Guid: [16]u8,
};

pub const PdbStreamVersion = enum(u32) {
    // zig fmt: off
    VC2     = 19941610,
    VC4     = 19950623,
    VC41    = 19950814,
    VC50    = 19960307,
    VC98    = 19970604,
    VC70Dep = 19990604,
    VC70    = 20000404,
    VC80    = 20030901,
    VC110   = 20091201,
    VC140   = 20140508,
    _,
    // zig fmt: on
};

pub const PdbFeatureCode = enum(u32) {
    // zig fmt: off
    VC110            = 20091201,
    VC140            = 20140508,
    NoTypeMerge      = 0x4D544F4E,
    MinimalDebugInfo = 0x494E494D,
    _,
    // zig fmt: on
};
