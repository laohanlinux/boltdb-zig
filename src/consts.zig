const std = @import("std");

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
pub const PageSize: usize = std.mem.page_size;

/// Returns the size of a page given the page size and branching factor.
pub fn intFromFlags(pageFlage: PageFlag) u16 {
    return @as(u16, @intFromEnum(pageFlage));
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

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

/// Get the global general purpose allocator.
pub fn getGpa() *std.heap.GeneralPurposeAllocator {
    return &gpa;
}

/// A string with reference counting.
pub const BufStr = struct {
    _str: []const u8,
    ref: *std.atomic.Value(i64),
    _allocator: ?std.mem.Allocator,

    /// Init a string.
    pub fn init(allocator: ?std.mem.Allocator, str: []const u8) @This() {
        const _allocator = allocator orelse gpa.allocator();
        const refValue = _allocator.create(std.atomic.Value(i64)) catch unreachable;
        refValue.store(1, .seq_cst);
        return .{ ._str = str, ._allocator = allocator, .ref = refValue };
    }

    /// Deinit a string.
    pub fn deinit(self: *@This()) void {
        const refValue = self.ref.fetchSub(1, .seq_cst);
        if (refValue < 1) {
            unreachable;
        }
        if (refValue == 1) {
            if (self._allocator) |allocator| {
                allocator.free(self._str);
                allocator.destroy(self.ref);
            } else {
                gpa.allocator().destroy(self.ref);
                std.debug.print("deinit\n", .{});
            }
            self.* = undefined;
        }
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

    /// Get the string as a slice.
    pub fn asSlice(self: *@This()) []const u8 {
        return self._str;
    }
};

test "String" {
    var str = try std.testing.allocator.alloc(u8, 3);
    defer std.testing.allocator.free(str);
    str[0] = 55;
    str[1] = 65;
    var str1 = BufStr.init(null, str);
    defer str1.deinit();
    var str2 = str1.clone();
    defer str2.deinit();
}
