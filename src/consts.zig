const std = @import("std");

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
