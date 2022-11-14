const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

/// A bit set with runtime-known size, backed by an allocated slice
/// of usize.  The allocator must be tracked externally by the user.
const Self = @This();

/// The integer type used to represent a mask in this bit set
pub const MaskInt = u32;

/// The integer type used to shift a mask in this bit set
pub const ShiftInt = std.math.Log2Int(MaskInt);

/// The number of valid items in this bit set
bit_length: usize = 0,

/// The bit masks, ordered with lower indices first.
/// Padding bits at the end must be zeroed.
masks: [*]MaskInt = empty_masks_ptr,
// This pointer is one usize after the actual allocation.
// That slot holds the size of the true allocation, which
// is needed by Zig's allocator interface in case a shrink
// fails.

// Don't modify this value.  Ideally it would go in const data so
// modifications would cause a bus error, but the only way
// to discard a const qualifier is through ptrToInt, which
// cannot currently round trip at comptime.
var empty_masks_data = [_]MaskInt{ 0, undefined };
const empty_masks_ptr = empty_masks_data[1..2];

/// A range of indices within a bitset.
pub const Range = struct {
    /// The index of the first bit of interest.
    start: usize,
    /// The index immediately after the last bit of interest.
    end: usize,
};

/// Options for configuring an iterator over a bit set
pub const IteratorOptions = struct {
    /// determines which bits should be visited
    kind: Type = .set,
    /// determines the order in which bit indices should be visited
    direction: Direction = .forward,

    pub const Type = enum {
        /// visit indexes of set bits
        set,
        /// visit indexes of unset bits
        unset,
    };

    pub const Direction = enum {
        /// visit indices in ascending order
        forward,
        /// visit indices in descending order.
        /// Note that this may be slightly more expensive than forward iteration.
        reverse,
    };
};

/// Creates a bit set with no elements present.
/// If bit_length is not zero, deinit must eventually be called.
pub fn initEmpty(allocator: Allocator, bit_length: usize) !Self {
    var self = Self{};
    try self.resize(allocator, bit_length, false);
    return self;
}

/// Creates a bit set with all elements present.
/// If bit_length is not zero, deinit must eventually be called.
pub fn initFull(allocator: Allocator, bit_length: usize) !Self {
    var self = Self{};
    try self.resize(allocator, bit_length, true);
    return self;
}

/// Resizes to a new bit_length.  If the new length is larger
/// than the old length, fills any added bits with `fill`.
/// If new_len is not zero, deinit must eventually be called.
pub fn resize(self: *@This(), allocator: Allocator, new_len: usize, fill: bool) !void {
    const old_len = self.bit_length;

    const old_masks = numMasks(old_len);
    const new_masks = numMasks(new_len);

    const old_allocation = (self.masks - 1)[0..(self.masks - 1)[0]];

    if (new_masks == 0) {
        assert(new_len == 0);
        allocator.free(old_allocation);
        self.masks = empty_masks_ptr;
        self.bit_length = 0;
        return;
    }

    if (old_allocation.len != new_masks + 1) realloc: {
        // If realloc fails, it may mean one of two things.
        // If we are growing, it means we are out of memory.
        // If we are shrinking, it means the allocator doesn't
        // want to move the allocation.  This means we need to
        // hold on to the extra 8 bytes required to be able to free
        // this allocation properly.
        const new_allocation = allocator.realloc(old_allocation, new_masks + 1) catch |err| {
            if (new_masks + 1 > old_allocation.len) return err;
            break :realloc;
        };

        new_allocation[0] = @intCast(u32, new_allocation.len);
        self.masks = new_allocation.ptr + 1;
    }

    // If we increased in size, we need to set any new bits
    // to the fill value.
    if (new_len > old_len) {
        // set the padding bits in the old last item to 1
        if (fill and old_masks > 0) {
            const old_padding_bits = old_masks * @bitSizeOf(MaskInt) - old_len;
            const old_mask = (~@as(MaskInt, 0)) >> @intCast(ShiftInt, old_padding_bits);
            self.masks[old_masks - 1] |= ~old_mask;
        }

        // fill in any new masks
        if (new_masks > old_masks) {
            const fill_value = std.math.boolMask(MaskInt, fill);
            std.mem.set(MaskInt, self.masks[old_masks..new_masks], fill_value);
        }
    }

    // Zero out the padding bits
    if (new_len > 0) {
        const padding_bits = new_masks * @bitSizeOf(MaskInt) - new_len;
        const last_item_mask = (~@as(MaskInt, 0)) >> @intCast(ShiftInt, padding_bits);
        self.masks[new_masks - 1] &= last_item_mask;
    }

    // And finally, save the new length.
    self.bit_length = new_len;
}

/// deinitializes the array and releases its memory.
/// The passed allocator must be the same one used for
/// init* or resize in the past.
pub fn deinit(self: *Self, allocator: Allocator) void {
    self.resize(allocator, 0, false) catch unreachable;
}

/// Creates a duplicate of this bit set, using the new allocator.
pub fn clone(self: *const Self, new_allocator: Allocator) !Self {
    const num_masks = numMasks(self.bit_length);
    var copy = Self{};
    try copy.resize(new_allocator, self.bit_length, false);
    std.mem.copy(MaskInt, copy.masks[0..num_masks], self.masks[0..num_masks]);
    return copy;
}

/// Returns the number of bits in this bit set
pub inline fn capacity(self: Self) usize {
    return self.bit_length;
}

/// Returns true if the bit at the specified index
/// is present in the set, false otherwise.
pub fn isSet(self: Self, index: usize) bool {
    assert(index < self.bit_length);
    return (self.masks[maskIndex(index)] & maskBit(index)) != 0;
}

/// Returns the total number of set bits in this bit set.
pub fn count(self: Self) usize {
    const num_masks = (self.bit_length + (@bitSizeOf(MaskInt) - 1)) / @bitSizeOf(MaskInt);
    var total: usize = 0;
    for (self.masks[0..num_masks]) |mask| {
        // Note: This is where we depend on padding bits being zero
        total += @popCount(mask);
    }
    return total;
}

/// Changes the value of the specified bit of the bit
/// set to match the passed boolean.
pub fn setValue(self: *Self, index: usize, value: bool) void {
    assert(index < self.bit_length);
    const bit = maskBit(index);
    const mask_index = maskIndex(index);
    const new_bit = bit & std.math.boolMask(MaskInt, value);
    self.masks[mask_index] = (self.masks[mask_index] & ~bit) | new_bit;
}

/// Adds a specific bit to the bit set
pub fn set(self: *Self, index: usize) void {
    assert(index < self.bit_length);
    self.masks[maskIndex(index)] |= maskBit(index);
}

/// Changes the value of all bits in the specified range to
/// match the passed boolean.
pub fn setRangeValue(self: *Self, range: Range, value: bool) void {
    assert(range.end <= self.bit_length);
    assert(range.start <= range.end);
    if (range.start == range.end) return;

    const start_mask_index = maskIndex(range.start);
    const start_bit = @truncate(ShiftInt, range.start);

    const end_mask_index = maskIndex(range.end);
    const end_bit = @truncate(ShiftInt, range.end);

    if (start_mask_index == end_mask_index) {
        var mask1 = std.math.boolMask(MaskInt, true) << start_bit;
        var mask2 = std.math.boolMask(MaskInt, true) >> (@bitSizeOf(MaskInt) - 1) - (end_bit - 1);
        self.masks[start_mask_index] &= ~(mask1 & mask2);

        mask1 = std.math.boolMask(MaskInt, value) << start_bit;
        mask2 = std.math.boolMask(MaskInt, value) >> (@bitSizeOf(MaskInt) - 1) - (end_bit - 1);
        self.masks[start_mask_index] |= mask1 & mask2;
    } else {
        var bulk_mask_index: usize = undefined;
        if (start_bit > 0) {
            self.masks[start_mask_index] =
                (self.masks[start_mask_index] & ~(std.math.boolMask(MaskInt, true) << start_bit)) |
                (std.math.boolMask(MaskInt, value) << start_bit);
            bulk_mask_index = start_mask_index + 1;
        } else {
            bulk_mask_index = start_mask_index;
        }

        while (bulk_mask_index < end_mask_index) : (bulk_mask_index += 1) {
            self.masks[bulk_mask_index] = std.math.boolMask(MaskInt, value);
        }

        if (end_bit > 0) {
            self.masks[end_mask_index] =
                (self.masks[end_mask_index] & (std.math.boolMask(MaskInt, true) << end_bit)) |
                (std.math.boolMask(MaskInt, value) >> ((@bitSizeOf(MaskInt) - 1) - (end_bit - 1)));
        }
    }
}

/// Removes a specific bit from the bit set
pub fn unset(self: *Self, index: usize) void {
    assert(index < self.bit_length);
    self.masks[maskIndex(index)] &= ~maskBit(index);
}

/// Flips a specific bit in the bit set
pub fn toggle(self: *Self, index: usize) void {
    assert(index < self.bit_length);
    self.masks[maskIndex(index)] ^= maskBit(index);
}

/// Flips all bits in this bit set which are present
/// in the toggles bit set.  Both sets must have the
/// same bit_length.
pub fn toggleSet(self: *Self, toggles: Self) void {
    assert(toggles.bit_length == self.bit_length);
    const num_masks = numMasks(self.bit_length);
    for (self.masks[0..num_masks]) |*mask, i| {
        mask.* ^= toggles.masks[i];
    }
}

/// Flips every bit in the bit set.
pub fn toggleAll(self: *Self) void {
    const bit_length = self.bit_length;
    // avoid underflow if bit_length is zero
    if (bit_length == 0) return;

    const num_masks = numMasks(self.bit_length);
    for (self.masks[0..num_masks]) |*mask| {
        mask.* = ~mask.*;
    }

    const padding_bits = num_masks * @bitSizeOf(MaskInt) - bit_length;
    const last_item_mask = (~@as(MaskInt, 0)) >> @intCast(ShiftInt, padding_bits);
    self.masks[num_masks - 1] &= last_item_mask;
}

/// Performs a union of two bit sets, and stores the
/// result in the first one.  Bits in the result are
/// set if the corresponding bits were set in either input.
/// The two sets must both be the same bit_length.
pub fn setUnion(self: *Self, other: Self) void {
    assert(other.bit_length == self.bit_length);
    const num_masks = numMasks(self.bit_length);
    for (self.masks[0..num_masks]) |*mask, i| {
        mask.* |= other.masks[i];
    }
}

/// Performs an intersection of two bit sets, and stores
/// the result in the first one.  Bits in the result are
/// set if the corresponding bits were set in both inputs.
/// The two sets must both be the same bit_length.
pub fn setIntersection(self: *Self, other: Self) void {
    assert(other.bit_length == self.bit_length);
    const num_masks = numMasks(self.bit_length);
    for (self.masks[0..num_masks]) |*mask, i| {
        mask.* &= other.masks[i];
    }
}

/// Finds the index of the first set bit.
/// If no bits are set, returns null.
pub fn findFirstSet(self: Self) ?usize {
    var offset: usize = 0;
    var mask = self.masks;
    while (offset < self.bit_length) {
        if (mask[0] != 0) break;
        mask += 1;
        offset += @bitSizeOf(MaskInt);
    } else return null;
    return offset + @ctz(mask[0]);
}

/// Finds the index of the first set bit, and unsets it.
/// If no bits are set, returns null.
pub fn toggleFirstSet(self: *Self) ?usize {
    var offset: usize = 0;
    var mask = self.masks;
    while (offset < self.bit_length) {
        if (mask[0] != 0) break;
        mask += 1;
        offset += @bitSizeOf(MaskInt);
    } else return null;
    const index = @ctz(mask[0]);
    mask[0] &= (mask[0] - 1);
    return offset + index;
}

/// Iterates through the items in the set, according to the options.
/// The default options (.{}) will iterate indices of set bits in
/// ascending order.  Modifications to the underlying bit set may
/// or may not be observed by the iterator.  Resizing the underlying
/// bit set invalidates the iterator.
pub fn iterator(self: *const Self, comptime options: IteratorOptions) Iterator(options) {
    const num_masks = numMasks(self.bit_length);
    const padding_bits = num_masks * @bitSizeOf(MaskInt) - self.bit_length;
    const last_item_mask = (~@as(MaskInt, 0)) >> @intCast(ShiftInt, padding_bits);
    return Iterator(options).init(self.masks[0..num_masks], last_item_mask);
}

pub fn Iterator(comptime options: IteratorOptions) type {
    return BitSetIterator(MaskInt, options);
}

fn maskBit(index: usize) MaskInt {
    return @as(MaskInt, 1) << @truncate(ShiftInt, index);
}
fn maskIndex(index: usize) usize {
    return index >> @bitSizeOf(ShiftInt);
}
fn boolMaskBit(index: usize, value: bool) MaskInt {
    return @as(MaskInt, @boolToInt(value)) << @intCast(ShiftInt, index);
}

pub fn numMasks(bit_length: usize) usize {
    return (bit_length + (@bitSizeOf(MaskInt) - 1)) / @bitSizeOf(MaskInt);
}

// The iterator is reusable between several bit set types
fn BitSetIterator(comptime MaskIntT: type, comptime options: IteratorOptions) type {
    const ShiftIntT = std.math.Log2Int(MaskIntT);
    const kind = options.kind;
    const direction = options.direction;
    return struct {
        const It = @This();

        // all bits which have not yet been iterated over
        bits_remain: MaskIntT,
        // all words which have not yet been iterated over
        words_remain: []const MaskIntT,
        // the offset of the current word
        bit_offset: usize,
        // the mask of the last word
        last_word_mask: MaskIntT,

        fn init(masks: []const MaskIntT, last_word_mask: MaskIntT) It {
            if (masks.len == 0) {
                return It{
                    .bits_remain = 0,
                    .words_remain = &[_]MaskIntT{},
                    .last_word_mask = last_word_mask,
                    .bit_offset = 0,
                };
            } else {
                var result = It{
                    .bits_remain = 0,
                    .words_remain = masks,
                    .last_word_mask = last_word_mask,
                    .bit_offset = if (direction == .forward) 0 else (masks.len - 1) * @bitSizeOf(MaskIntT),
                };
                result.nextWord(true);
                return result;
            }
        }

        /// Returns the index of the next unvisited set bit
        /// in the bit set, in ascending order.
        pub fn next(self: *It) ?usize {
            while (self.bits_remain == 0) {
                if (self.words_remain.len == 0) return null;
                self.nextWord(false);
                switch (direction) {
                    .forward => self.bit_offset += @bitSizeOf(MaskIntT),
                    .reverse => self.bit_offset -= @bitSizeOf(MaskIntT),
                }
            }

            switch (direction) {
                .forward => {
                    const next_index = @ctz(self.bits_remain) + self.bit_offset;
                    self.bits_remain &= self.bits_remain - 1;
                    return next_index;
                },
                .reverse => {
                    const leading_zeroes = @clz(self.bits_remain);
                    const top_bit = (@bitSizeOf(MaskIntT) - 1) - leading_zeroes;
                    const no_top_bit_mask = (@as(MaskIntT, 1) << @intCast(ShiftIntT, top_bit)) - 1;
                    self.bits_remain &= no_top_bit_mask;
                    return top_bit + self.bit_offset;
                },
            }
        }

        // Load the next word.  Don't call this if there
        // isn't a next word.  If the next word is the
        // last word, mask off the padding bits so we
        // don't visit them.
        inline fn nextWord(self: *It, comptime is_first_word: bool) void {
            var word = switch (direction) {
                .forward => self.words_remain[0],
                .reverse => self.words_remain[self.words_remain.len - 1],
            };
            switch (kind) {
                .set => {},
                .unset => {
                    word = ~word;
                    if ((direction == .reverse and is_first_word) or
                        (direction == .forward and self.words_remain.len == 1))
                    {
                        word &= self.last_word_mask;
                    }
                },
            }
            switch (direction) {
                .forward => self.words_remain = self.words_remain[1..],
                .reverse => self.words_remain.len -= 1,
            }
            self.bits_remain = word;
        }
    };
}
