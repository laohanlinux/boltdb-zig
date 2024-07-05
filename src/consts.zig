const std = @import("std");

pub const BucketLeafFlag: u32 = 0x01;

pub const minFillPercent: f64 = 0.1;
pub const maxFillPercent: f64 = 1.0;

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
