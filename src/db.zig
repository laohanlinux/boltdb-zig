const std = @import("std");
const page = @import("./page.zig");
const tx = @import("./tx.zig");
const errors = @import("./error.zig");
const bucket = @import("./bucket.zig");

// Represents a marker value to indicate that a file is a Bolt DB.
const Magic = 0xED0CDAED;
// The data file format verison.
const Version = 1;

// The largest step that can be taken when remapping the mmap.
const MaxMMapStep = 1 << 30; // 1 GB

// TODO
const IgnoreNoSync = false;

// Default values if not set in a DB instance.
const DefaultMaxBatchSize = 1000;
const DefaultMaxBatchDelay = 10; // millisecond
const DefaultAllocSize = 16 * 1024 * 1024;

// Page size for db is set to the OS page size.
const default_page_size = std.os.getPageSize();

pub const DB = struct {
    // When enabled, the database will perform a check() after every commit.
    // A painic if issued if the database is in an inconsistent stats. This
    // flag has a large performance impact so it should only be used for debugging purposes.
    strict_mode: bool,

    // Setting the no_sync flag will cause the database to skip fsync()
    // calls after each commit. This can be useful when bulk loading data
    // into a database and you can restart the bulk load in the event of
    // a system failure or database corruption. Do not set this flag for
    // normal use.
    //
    // If the package global IgnoreNoSync constant is true, this value is
    // ignored.  See the comment on that constant for more details.
    //
    // THIS IS UNSAFE. PLEASE USE WITH CAUTION.
    no_sync: bool,

    // When true, skips the truncate call when growing the database.
    // Setting this to true is only safe on non-ext3/ext4 systems.
    // Skipping truncation avoids pareallocation of hard drive space and
    // bypasssing a truncate() and fsync() syscall on remapping.
    //
    no_grow_sync: bool,

    // If you want to read the entire database fast. you can set MMAPFLAG to
    // syscall.MAP_POPULATE on linux 2.6.23+ for sequential read-ahead.
    mmap_flags: isize,

    // MaxBatchSize is the maximum size of a batch. Default value is
    // copied from DefaultMaxBatchSize in open.
    //
    // If <=0, disables batching.
    //
    // Do not change concurrently with calls to Batch.
    max_batch_size: isize,

    // MaxBatchDelay is the maximum delay before a batch starts.
    // Default value is copied from DefaultMaxBatchDelay in open.
    //
    // If <= 0, effectively disable batching.
    //
    // Do not change currently with calls to Batch,
    max_batch_delay: isize, // millis

    // AllocSize is the amount of space allocated when the database
    // needs to create new pages. This is done to amortize the cost
    // of truncate() and fsync() when growing the data file.
    alloc_size: isize,
};

// Represents the options that can be set when opening a database.
pub const Options = packed struct {
    // The amount of time to what wait to obtain a file lock.
    // When set to zero it will wait indefinitely. This option is only
    // available on Darwin and Linux.
    timeout: i64, // unit:nas

    // Sets the DB.no_grow_sync flag before money mapping the file.
    no_grow_sync: bool,

    // Open database in read-only mode, Uses flock(..., LOCK_SH | LOCK_NB) to
    // grab a shared lock (UNIX).
    read_only: bool,

    // Sets the DB.mmap_flags before memory mapping the file.
    mmap_flags: isize,

    // The initial mmap size of the database
    // in bytes. Read transactions won't block write transaction
    // if the initial_mmap_size is large enough to hold database mmap
    // size. (See DB.begin for more information)
    //
    // If <= 0, the initial map size is 0.
    // If initial_mmap_size is smaller than the previous database size.
    // it takes no effect.
    initial_mmap_size: isize,
};

// Represents the options used if null options are passed into open().
// No timeout is used which will cause Bolt to wait indefinitely for a lock.
pub const default_options = Options{
    .timeout = 0,
    .no_grow_sync = false,
};

// Represents statistics about the database
pub const Stats = packed struct {
    // freelist stats
    free_page_n: usize, // total number of free pages on the freelist
    pending_page_n: usize, // total number of pending pages on the freelist
    free_alloc: usize, // total bytes allocated in free pages
    free_list_inuse: usize, // total bytes used by the freelist

    // Transaction stats
    tx_n: usize, // total number of started read transactions
    open_tx_n: usize, // number of currently open read transactions

    tx_stats: tx.TxStats, // global, ongoing stats

    const Self = @This();

    pub fn sub(self: *Self, other: *Stats) Stats {
        if (other == null) {
            return self.*;
        }
        var diff = Stats{
            .free_page_n = self.free_page_n,
            .pending_page_n = self.pending_page_n,
            .free_alloc = self.free_alloc,
            .free_list_inuse = self.free_list_inuse,
            .tx_n = self.tx_n - other.tx_n,
            .tx_stats = self.tx_stats.sub(other.tx_stats),
        };
        return diff;
    }

    pub fn add(self: *Self, other: *Stats) void {
        self.tx_stats.add(other.tx_stats);
    }
};

pub const Info = packed struct {
    data: usize,
    page_size: usize,
};

pub const Meta = packed struct {
    magic: u32,
    version: u32,
    page_size: u32,
    flags: u32,
    root: bucket._Bucket,
    free_list: page.pgid_type,
    pgid: page.pgid_type,
    txid: tx.TxId,
    check_sum: u64,

    const Self = @This();
    pub const header_size = @sizeOf(Meta);

    pub fn validate(self: *Self) errors.Error!void {
        if (self.magic != Magic) {
            return errors.Error.Invalid;
        } else if (self.version != Version) {
            return errors.Error.VersionMismatch;
        } else if (self.check_sum != 0 and self.check_sum != self.sum64()) {
            return errors.Error.CheckSum;
        }

        return;
    }

    pub fn sum64(self: *Self) u64 {
        const ptr = @fieldParentPtr(Meta, "check_sum", self);
        const slice = @as([ptr - self]u8, @ptrFromInt(ptr));
        const crc32 = std.hash.Crc32.hash(slice);
        return @as(crc32, u64);
    }

    // Writes the meta onto a page.
    pub fn write(self: *Self, p: *page.Page) void {
        if (self.root.root >= self.pgid) {
            unreachable;
        } else if (self.free_list >= self.pgid) {
            unreachable;
        }
        // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
        p.id = @as(page.pgid_type, self.txid % 2);
        p.flags |= page.PageFlage.meta;

        // Calculate the checksum.
        self.check_sum = self.sum64();
        var meta = p.meta();
        meta.* = self.*;
        return;
    }
};

test "meta" {
    const stats = Info{ .data = 10, .page_size = 20 };
    std.debug.print("{}\n", .{stats});
}
