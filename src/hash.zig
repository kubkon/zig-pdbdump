//! Hashes used by Microsoft in PDB file format.

/// Corresponds to LLVM's `hashStringV1` and Microsoft's `Hasher::lhashPbCb`.
/// This hash is used for name hash table (such as NamedStreamMap) and TPI/IPI hashes.
pub fn hashStringV1(str: []const u8) u32 {
    var result: u32 = 0;

    const longs = @ptrCast([*]align(1) const u32, str.ptr)[0..@divTrunc(str.len, 4)];
    for (longs) |val| {
        result ^= val;
    }

    var remainder_pos = longs.len * @sizeOf(u32);
    var remainder_size = str.len % 4;

    if (remainder_size >= 2) {
        const val = @ptrCast(*align(1) const u16, str.ptr + remainder_pos).*;
        result ^= val;
        remainder_pos += 2;
        remainder_size -= 2;
    }

    if (remainder_size == 1) {
        result ^= str[remainder_pos];
    }

    const to_lower_mask = 0x20202020;
    result |= to_lower_mask;
    result ^= (result >> 11);
    result ^= (result >> 16);

    return result;
}

/// Corresponds to LLVM's `hashStringV2` and Microsoft's `HasherV2::HashULONG`.
/// This hash is used for name hash table.
pub fn hashStringV2(str: []const u8) u32 {
    var result: u32 = 0xb170a1bf;

    const items = @ptrCast([*]align(1) const u32, str.ptr)[0..@divTrunc(str.len, @sizeOf(u32))];
    for (items) |item| {
        result +%= item;
        result +%= (result << 10);
        result ^= (result >> 6);
    }

    const remainder = str[items.len * @sizeOf(u32) ..];
    for (remainder) |item| {
        result +%= item;
        result +%= (result << 10);
        result ^= (result >> 6);
    }

    return result *% 1664525 +% 1013904223;
}
