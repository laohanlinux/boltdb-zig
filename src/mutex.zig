const std = @import("std");
const Semaphore = std.Thread.Semaphore;

/// A mutex that can be shared between threads.
pub const Mutex = struct {
    sem: Semaphore = Semaphore{ .permits = 1 },

    /// Initialize the mutex.
    pub fn init() Mutex {
        return .{ .sem = Semaphore{ .permits = 1 } };
    }

    /// Lock the mutex.
    pub fn lock(self: *@This()) void {
        self.sem.wait();
    }

    /// Unlock the mutex.
    pub fn unlock(self: *@This()) void {
        self.sem.post();
    }

    /// Try to lock the mutex.
    pub fn tryLock(self: *@This(), timeout_ns: usize) error{Timeout}!void {
        return self.sem.timedWait(timeout_ns);
    }
};
