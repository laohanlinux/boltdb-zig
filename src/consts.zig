const std = @import("std");
const assert = @import("util.zig").assert;
const Page = @import("page.zig").Page;
const Node = @import("node.zig").Node;
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
    freeList = 0x10,
};
/// A bucket leaf flag.
pub const bucket_leaf_flag: u32 = 0x01;
/// A page id type.
pub const PgidType = u64;
/// A slice of page ids.
pub const PgIds = []PgidType;
/// The size of a page.
pub const PageSize: usize = std.mem.page_size;
// pub const PageSize: usize = 512;

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

    @panic("invalid flag");
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

    fn dupeValue(self: *const KeyValueRef, allocator: std.mem.Allocator) ?[]u8 {
        if (self.value) |value| {
            return allocator.dupe(u8, value) catch unreachable;
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

// global general purpose allocator
const gpa = std.heap.GeneralPurposeAllocator(.{}){};

/// Get the global general purpose allocator.
pub fn getGpa() *std.heap.GeneralPurposeAllocator {
    return &gpa;
}
