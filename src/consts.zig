const std = @import("std");
const util = @import("util.zig");
const assert = util.assert;
const panicFmt = util.panicFmt;
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

/// A mutex that can be shared between threads.
pub const SharedMutex = struct {
    sem: std.Thread.Semaphore,
    timeout: u64 = std.time.ns_per_s * 1,
    /// Initialize the mutex.
    pub fn init() SharedMutex {
        return .{
            .sem = std.Thread.Semaphore{ .permits = 1 },
        };
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
    pub fn tryLock(self: *@This()) bool {
        return self.sem.timedWait(self.timeout) catch .{ .timed_out = {} };
    }
};

/// A mutex that allows multiple readers but only one writer.
/// This is a reader-writer mutex implementation using semaphores and atomic operations.
/// TODO: add a timeout for the mutex and add a priority for the writer or reader,
/// avoid writer, reader starvation.
pub const RxMutex = struct {
    write_sem: std.Thread.Semaphore,
    read_sem: std.Thread.Semaphore,
    readers: std.atomic.Value(u32),
    writer: std.atomic.Value(bool),
    /// Initialize the mutex.
    pub fn init() RxMutex {
        return .{
            .write_sem = std.Thread.Semaphore{ .permits = 1 },
            .read_sem = std.Thread.Semaphore{ .permits = 1 },
            .readers = std.atomic.Value(u32).init(0),
            .writer = std.atomic.Value(bool).init(false),
        };
    }

    /// Lock the mutex for reading.
    pub fn lockShared(self: *@This()) void {
        // wait for a reader to release the lock
        self.read_sem.wait();
        // wait for the writer to release the lock
        while (self.writer.load(.Acquire)) {
            // release the read lock, and wait for the writer to release the lock
            self.read_sem.post();
            // avoid busy-waiting
            std.Thread.yield();
            // wait for a reader to release the lock again
            self.read_sem.wait();
        }
        // get the read lock.
        const readers = self.readers.fetchAdd(1, .Release);
        if (readers == 0) {
            // the first reader, wait for the writer to release the lock
            // this is to ensure that the writer is not blocked by readers
            // post the write sem when the last reader releases the lock
            self.write_sem.wait();
        }
        // release the read lock, so other readers can get the lock
        self.read_sem.post();
    }

    /// Unlock the mutex for reading.
    pub fn unlockShared(self: *@This()) void {
        const readers = self.readers.fetchSub(1, .Release);
        if (readers == 1) {
            // the last reader, post the write sem
            self.write_sem.post(); // Allow the writer to acquire the lock
        }
    }

    /// Lock the mutex for writing.
    pub fn lockExclusive(self: *@This()) void {
        self.write_sem.wait(); // Wait for the writer to release the lock
        self.writer.store(true, .Release); // Set the writer flag
    }

    /// Unlock the mutex for writing.
    pub fn unlockExclusive(self: *@This()) void {
        self.writer.store(false, .Release); // Clear the writer flag
        self.write_sem.post(); // Allow other readers to acquire the lock
    }
};

/// A channel that can be used to send and receive values between threads.
pub fn Channel(comptime T: type) type {
    return struct {
        mutex: std.Thread.Mutex,
        not_empty: std.Thread.Condition,
        not_full: std.Thread.Condition,
        buffer: std.ArrayList(T),
        capacity: usize,
        closed: bool,

        const Self = @This();

        /// Initialize a new channel with the given capacity.
        pub fn init(allocator: std.mem.Allocator, capacity: usize) Self {
            return .{
                .mutex = .{},
                .not_empty = .{},
                .not_full = .{},
                .buffer = std.ArrayList(T).init(allocator),
                .capacity = capacity,
                .closed = false,
            };
        }

        /// Send a value to the channel.
        pub fn send(self: *Self, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) {
                return error.ChannelClosed;
            }

            // wait until the there's space in the buffer.
            while (self.buffer.items.len >= self.capacity) {
                self.not_full.wait(&self.mutex);
                if (self.closed) {
                    return error.ChannelClosed;
                }
                try std.Thread.yield();
            }

            // add the value to the buffer.
            try self.buffer.append(value);
            self.not_empty.signal();
        }

        /// Try to send a value to the channel.
        pub fn trySend(self: *Self, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) {
                return error.ChannelClosed;
            }

            // If there's space in the buffer, add the value.
            if (self.buffer.items.len >= self.capacity) {
                return error.ChannelFull;
            }

            // Add the value to the buffer.
            try self.buffer.append(value);
            self.not_empty.signal();
        }

        /// Receive a value from the channel.
        pub fn recv(self: *Self) !T {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Wait until there's data to receive.
            while (self.buffer.items.len == 0) {
                if (self.closed) {
                    return error.ChannelClosed;
                }
                self.not_empty.wait(&self.mutex);
                try std.Thread.yield();
            }

            const value = self.buffer.orderedRemove(0);
            self.not_full.signal();
            return value;
        }

        /// Try to receive a value from the channel.
        pub fn tryRecv(self: *Self) !T {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.closed) {
                return error.ChannelClosed;
            }

            // Check if channel is empty.
            if (self.buffer.items.len == 0) {
                return error.ChannelEmpty;
            }

            // Remove the value from the buffer.
            const value = self.buffer.orderedRemove(0);
            self.not_full.signal();
            return value;
        }

        /// Close the channel.
        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (!self.closed) {
                self.closed = true;
                self.not_empty.broadcast();
                self.not_full.broadcast();
            }
        }

        /// Deinitialize the channel.
        pub fn deinit(self: *Self) void {
            self.buffer.deinit();
        }
    };
}

// test "SharedMutex cross-thread unlock" {
//     std.testing.log_level = .debug;
//     var mutex = SharedMutex.init();

//     // create a thread to get the lock
//     const thread = try std.Thread.spawn(.{}, struct {
//         fn run(_mutex: *SharedMutex) void {
//             _mutex.lock();
//             std.log.debug("thread: locked", .{});
//             std.time.sleep(std.time.ns_per_s * 2); // sleep for 2 seconds
//             std.log.debug("thread: still holding lock", .{});
//         }
//     }.run, .{&mutex});

//     // wait for the thread to get the lock
//     std.time.sleep(std.time.ns_per_s * 1);

//     // unlock the mutex from the main thread (cross-thread unlock)
//     std.log.debug("main: unlocking from different thread", .{});
//     mutex.unlock();
//     std.log.debug("main: unlocked successfully", .{});

//     // wait for the thread to finish
//     thread.join();
// }

test "Channel" {
    var channel = Channel(usize).init(std.testing.allocator, 5);
    defer channel.deinit();

    try channel.send(1);
    try channel.send(2);
    try channel.send(3);
    try channel.send(4);
    try channel.send(5);
    channel.trySend(6) catch |err| {
        std.debug.assert(err == error.ChannelFull);
    };

    for (0..5) |i| {
        const received = try channel.tryRecv();
        std.debug.assert(received == i + 1);
    }

    _ = channel.tryRecv() catch |err| {
        std.debug.assert(err == error.ChannelEmpty);
    };

    // mutil send
    for (0..10) |_| {
        _ = try std.Thread.spawn(.{}, struct {
            fn run(_channel: *Channel(usize)) void {
                _channel.send(0) catch unreachable;
            }
        }.run, .{&channel});
    }

    std.time.sleep(std.time.ns_per_s * 1);
    for (0..10) |_| {
        const value = try channel.recv();
        std.debug.assert(value == 0);
    }
    channel.close();
    _ = channel.recv() catch |err| {
        std.debug.assert(err == error.ChannelClosed);
    };
}
