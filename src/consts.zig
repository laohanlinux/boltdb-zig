const std = @import("std");
const util = @import("util.zig");
const assert = util.assert;
const panicFmt = util.panicFmt;
const Page = @import("page.zig").Page;
const Node = @import("node.zig").Node;
/// Represents a marker value to indicate that a file is a Bolt DB.
pub const Magic = 0xED0CDAED;
/// The data file format verison.
pub const Version = 2;

/// The largest step that can be taken when remapping the mmap.
pub const MaxMMapStep: u64 = 1 << 30; // 1 GB

/// Default values if not set in a DB instance.
pub const DefaultMaxBatchSize = 1000; // not used yet
pub const DefaultMaxBatchDelay = 10 * std.time.ms_per_s; // millisecond, not used yet
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
    freeList = 0x10,
};
/// A bucket leaf flag.
pub const bucket_leaf_flag: u32 = 0x01;
/// A page id type.
pub const PgidType = u64;
/// A slice of page ids.
pub const PgIds = []PgidType;
/// The size of a page.
pub const PageSize: usize = std.heap.page_size_min;
// pub const PageSize: usize = 4096;

/// Represents the options that can be set when opening a database.
pub const Options = packed struct {
    // The amount of time to what wait to obtain a file lock.
    // When set to zero it will wait indefinitely. This option is only
    // available on Darwin and Linux.
    timeout: i64 = 0, // unit:nas

    // Sets the DB.no_grow_sync flag before money mapping the file.
    noGrowSync: bool = false,

    // Open database in read-only mode, Uses flock(..., LOCK_SH | LOCK_NB) to
    // grab a shared lock (UNIX).
    readOnly: bool = false,

    // Sets the DB.strict_mode flag before memory mapping the file.
    strictMode: bool = false,

    // Sets the DB.mmap_flags before memory mapping the file.
    mmapFlags: isize = 0,

    // The initial mmap size of the database
    // in bytes. Read transactions won't block write transaction
    // if the initial_mmap_size is large enough to hold database mmap
    // size. (See DB.begin for more information)
    //
    // If <= 0, the initial map size is 0.
    // If initial_mmap_size is smaller than the previous database size.
    // it takes no effect.
    initialMmapSize: usize = 0,
    // The page size of the database, it only use to test, don't set at in production
    pageSize: usize = 0,
};

/// Represents the options used if null options are passed into open().
/// No timeout is used which will cause Bolt to wait indefinitely for a lock.
pub const defaultOptions = Options{
    .timeout = 0,
    .noGrowSync = false,
};

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
        return PageFlag.freeList;
    }

    assert(false, "invalid flag: {}", .{flag});
    @panic("");
}

/// Represents the internal transaction indentifier.
pub const TxId = u64;

/// A page or node.
pub const PageOrNode = struct {
    page: ?*Page,
    node: ?*Node,
};

/// A key-value reference.
pub const KeyValueRef = struct {
    key: ?[]const u8 = null,
    value: ?[]u8 = null,
    flag: u32 = 0,
    pub fn dupeKey(self: *const KeyValueRef, allocator: std.mem.Allocator) ?[]const u8 {
        if (self.key) |key| {
            return allocator.dupe(u8, key) catch unreachable;
        }
        return null;
    }
};

/// A key-value pair.
pub const KeyPair = struct {
    key: ?[]const u8,
    value: ?[]const u8,
    /// Create a new key-value pair.
    pub fn init(key: ?[]const u8, value: ?[]const u8) @This() {
        return KeyPair{ .key = key, .value = value };
    }

    /// Check if the key is not found.
    pub fn isNotFound(self: *const KeyPair) bool {
        return self.key == null;
    }

    /// Check if the value is a bucket.
    pub fn isBucket(self: *const KeyPair) bool {
        return !self.isNotFound() and self.value == null;
    }
};

/// Calculate the threshold before starting a new node.
pub fn calThreshold(fillPercent: f64, pageSize: usize) usize {
    const _fillPercent = if (fillPercent < MinFillPercent) MinFillPercent else if (fillPercent > MaxFillPercent) MaxFillPercent else fillPercent;
    const fPageSize: f64 = @floatFromInt(pageSize);
    const threshold = @as(usize, @intFromFloat(fPageSize * _fillPercent));
    return threshold;
}
