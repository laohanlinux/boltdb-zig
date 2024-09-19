const std = @import("std");
const Thread = std.Thread;
const Mutex = Thread.Mutex;
const spawn = Thread.spawn;
const SpawnConfig = Thread.SpawnConfig;

pub const SharedData = struct {
    mutex: Mutex,
    value: i32,

    const Self = @This();
};
