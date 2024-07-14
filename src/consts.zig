const std = @import("std");

pub const BucketLeafFlag: u32 = 0x01;

pub const minFillPercent: f64 = 0.1;
pub const maxFillPercent: f64 = 1.0;

/// The maximum length of a key, in bytes
pub const MaxKeySize: usize = 32768;
/// The maximum length of a value, in bytes
pub const MaxValueSize: usize = (1 << 32) - 2;

// The percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
pub const defaultFillPercent = 0.5;

pub const PageFlag = enum(u8) {
    branch = 0x01,
    leaf = 0x02,
    meta = 0x04,
    free_list = 0x10,
};

pub const bucket_leaf_flag: u32 = 0x01;

pub const PgidType = u64;

pub const PgIds = []PgidType;

pub const PageSize: usize = std.mem.page_size;

/// Returns the size of a page given the page size and branching factor.
pub fn intFromFlags(pageFlage: PageFlag) u16 {
    return @as(u16, @intFromEnum(pageFlage));
}

// Represents the internal transaction indentifier.
pub const TxId = u64;

pub const Tuple = struct {
    pub fn t2(comptime firstType: type, comptime secondType: type) type {
        return struct {
            first: firstType,
            second: secondType,
        };
    }
    pub fn t3(comptime firstType: type, comptime secondType: type, comptime thirdType: type) type {
        return struct {
            first: firstType,
            second: secondType,
            third: thirdType,
        };
    }
};

pub const KeyPair = struct {
    key: ?[]u8,
    value: ?[]u8,

    pub fn init(_key: ?[]u8, _value: ?[]u8) KeyPair {
        return KeyPair{ .key = _key, .value = _value };
    }
};
