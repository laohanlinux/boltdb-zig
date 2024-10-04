const std = @import("std");
const assert = @import("util.zig").assert;
/// Represents a marker value to indicate that a file is a Bolt DB.
pub const Magic = 0xED0CDAED;
/// The data file format verison.
pub const Version = 1;

/// The largest step that can be taken when remapping the mmap.
pub const MaxMMapStep: u64 = 1 << 30; // 1 GB

/// Default values if not set in a DB instance.
pub const DefaultMaxBatchSize = 1000;
pub const DefaultMaxBatchDelay = 10; // millisecond
pub const DefaultAllocSize = 16 * 1024 * 1024;

/// A bucket leaf flag.
pub const BucketLeafFlag: u32 = 0x01;

pub const MinFillPercent: f64 = 0.1;
pub const MaxFillPercent: f64 = 1.0;

/// The maximum length of a key, in bytes
pub const MaxKeySize: usize = 32768;
/// The maximum length of a value, in bytes
pub const MaxValueSize: usize = (1 << 32) - 2;

/// The percentage that split pages are filled.
/// This value can be changed by setting Bucket.FillPercent.
pub const DefaultFillPercent = 0.5;

/// The minimum number of keys in a page.
pub const MinKeysPage: usize = 2;

/// A page flag.
pub const PageFlag = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    meta = 0x04,
    free_list = 0x10,
};
/// A bucket leaf flag.
pub const bucket_leaf_flag: u32 = 0x01;
/// A page id type.
pub const PgidType = u64;
/// A slice of page ids.
pub const PgIds = []PgidType;
/// The size of a page.
// pub const PageSize: usize = std.mem.page_size;
pub const PageSize: usize = 512;

/// Returns the size of a page given the page size and branching factor.
pub fn intFromFlags(pageFlage: PageFlag) u16 {
    return @as(u16, @intFromEnum(pageFlage));
}

/// Convert 'flag' to PageFlag enum.
pub fn toFlags(flag: u16) PageFlag {
    if (flag == 0x01) {
        return PageFlag.branch;
    }
    if (flag == 0x02) {
        return PageFlag.leaf;
    }

    if (flag == 0x04) {
        return PageFlag.meta;
    }

    if (flag == 0x10) {
        return PageFlag.free_list;
    }

    @panic("invalid flag");
}

/// Represents the internal transaction indentifier.
pub const TxId = u64;

/// A tuple of two elements.
pub const Tuple = struct {
    /// A tuple of two elements.
    pub fn t2(comptime firstType: type, comptime secondType: type) type {
        return struct {
            first: firstType,
            second: secondType,
        };
    }

    /// A tuple of three elements.
    pub fn t3(comptime firstType: type, comptime secondType: type, comptime thirdType: type) type {
        return struct {
            first: firstType,
            second: secondType,
            third: thirdType,
        };
    }
};

/// A key-value pair.
pub const KeyPair = struct {
    key: ?[]const u8,
    value: ?[]const u8,
    /// Create a new key-value pair.
    pub fn init(_key: ?[]const u8, _value: ?[]const u8) @This() {
        return KeyPair{ .key = _key, .value = _value };
    }
};

// global general purpose allocator
const gpa = std.heap.GeneralPurposeAllocator(.{}){};

/// Get the global general purpose allocator.
pub fn getGpa() *std.heap.GeneralPurposeAllocator {
    return &gpa;
}

/// A string with reference counting.
pub const BufStr = struct {
    _str: ?[]const u8,
    ref: *std.atomic.Value(i64),
    _allocator: std.mem.Allocator,
    hash: u64 = 0,

    /// Init a string.
    pub fn init(allocator: std.mem.Allocator, str: ?[]const u8) @This() {
        const _allocator = allocator;
        const refValue = _allocator.create(std.atomic.Value(i64)) catch unreachable;
        refValue.store(1, .seq_cst);
        var _hash: u64 = 0;
        if (str) |_str| {
            _hash = std.hash.Wyhash.hash(0, _str);
        }
        std.log.debug("init BufStr[{d}]\n", .{_hash});
        return .{ ._str = str, ._allocator = allocator, .ref = refValue, .hash = _hash };
    }

    /// Increment the reference count.
    pub fn incrRef(self: *@This()) void {
        _ = self.ref.fetchAdd(1, .seq_cst);
    }

    /// Subtract the reference count.
    pub fn subRef(self: *@This()) void {
        _ = self.ref.fetchSub(1, .seq_cst);
    }

    /// Dupe a string from a slice.
    pub fn dupeFromSlice(allocator: std.mem.Allocator, str: ?[]const u8) @This() {
        const refValue = allocator.create(std.atomic.Value(i64)) catch unreachable;
        refValue.store(1, .seq_cst);
        if (str == null) {
            return .{ ._str = null, ._allocator = allocator, .ref = refValue };
        }
        const newStr = allocator.dupe(u8, str.?) catch unreachable;
        return .{ ._str = newStr, ._allocator = allocator, .ref = refValue };
    }

    /// Dupe a string to a slice.
    pub fn dupeToSlice(self: *@This(), allocator: ?std.mem.Allocator) ?[]u8 {
        if (self._str == null) {
            return null;
        }
        const _allocator = allocator orelse gpa.allocator();
        return _allocator.dupe(u8, self._str.?) catch unreachable;
    }

    /// Deinit a string.
    pub fn deinit(self: *@This()) void {
        const refValue = self.ref.fetchSub(1, .seq_cst);
        assert(refValue > 0, "reference count is less than 1, refValue: {d}", .{refValue});
        if (refValue == 1) {
            if (self._str) |str| {
                std.log.debug("free BufStr[{d}]: {s}, size: {d}\n", .{ self.hash, str, str.len });
                self._allocator.free(str);
                self._str = null;
            }
            self._allocator.destroy(self.ref);
            self.* = undefined;
        }
    }

    /// Destroy a string.
    pub fn destroy(self: *@This()) void {
        self.deinit();
    }

    /// Clone a string.
    pub fn clone(self: *@This()) @This() {
        _ = self.ref.fetchAdd(1, .seq_cst);
        return .{
            ._allocator = self._allocator,
            ._str = self._str,
            .ref = self.ref,
        };
    }

    /// Copy a string.
    pub fn copy(self: @This(), allocator: std.mem.Allocator) @This() {
        const _ref = allocator.create(std.atomic.Value(i64)) catch unreachable;
        _ref.store(1, .seq_cst);
        if (self._str == null) {
            return .{
                ._allocator = allocator,
                ._str = null,
                .ref = _ref,
            };
        }
        return .{
            ._allocator = allocator,
            ._str = allocator.dupe(u8, self._str.?) catch unreachable,
            .ref = _ref,
        };
    }

    /// Get the string as a slice.
    pub fn asSlice(self: *const @This()) ?[]const u8 {
        return self._str;
    }

    /// Get the string as a slice.
    pub fn asSliceZ(self: *const @This()) ?[]u8 {
        if (self._str == null) {
            return null;
        }
        const str = self._str.?;
        return @constCast(str);
    }

    /// Get the length of the string.
    pub fn len(self: *const @This()) usize {
        if (self._str == null) {
            return 0;
        }
        return self._str.?.len;
    }

    /// Hash a string.
    pub fn hash(self: @This()) u64 {
        if (self._str == null) {
            return 0;
        }
        return std.hash.Wyhash.hash(0, self._str.?);
    }

    /// Compare two strings.
    pub fn eql(self: @This(), other: @This()) bool {
        if (self._str == null) {
            return other._str == null;
        }
        if (other._str == null) {
            return false;
        }
        return std.mem.eql(u8, self._str.?, other._str.?);
    }
};
