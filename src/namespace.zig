const page = @import("page.zig");
const tx = @import("tx.zig");
const db = @import("db.zig");
const std = @import("std");
const consts = @import("consts.zig");
const DB = db.DB;
pub const Error = db.Error;
pub const Stats = db.Stats;
pub const BucketStats = @import("bucket.zig").BucketStats;
pub const TxStats = tx.TxStats;
pub const PageInfo = page.PageInfo;
pub const Options = consts.Options;
pub const defaultOptions = consts.defaultOptions;

/// A bucket is a collection of key-value pairs.
pub const Bucket = struct {
    _bt: *@import("bucket.zig").Bucket,
    const Self = @This();

    /// Retrieves a nested bucket by name.
    /// Returns nil if the bucket does not exits.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn bucket(self: Self, name: []const u8) ?Self {
        if (self._bt.getBucket(name)) |bt| {
            return .{ ._bt = bt };
        }
        return null;
    }

    /// Creates a new bucket at the given key and returns the new bucket.
    /// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucket(self: Self, key: []const u8) Error!Bucket {
        if (self._bt.createBucket(key)) |bt| {
            return .{ ._bt = bt };
        }
        return null;
    }

    /// Creates a new bucket if it doesn't already exist and returns a reference to it.
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucketIfNotExists(self: Self, key: []const u8) Error!Bucket {
        if (self._bt.createBucketIfNotExists(key)) |bt| {
            return .{ ._bt = bt };
        }
        return null;
    }

    /// Deletes a bucket at the give key.
    /// Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
    pub fn deleteBucket(self: *Self, key: []const u8) Error!void {
        return self._bt.deleteBucket(key);
    }

    /// Retrives the value for a key in the bucket.
    /// Return a nil value if the key does not exist or if the key is a nested bucket.
    /// The returned value is only valid for the life of the transaction.
    pub fn get(self: *Self, key: []const u8) ?[]u8 {
        return self._bt.get(key);
    }

    /// Sets the value for a key in the bucket.
    /// If the key exist then its previous value will be overwritten.
    /// Supplied value must remain valid for the life of the transaction.
    /// Returns an error if the bucket was created from a read-only transaction, if the key is bucket, if the key is too large, or
    /// of if the value is too large.
    pub fn put(self: *Self, key: []const u8, value: []const u8) Error!void {
        const keyPair = consts.KeyPair{ .key = key, .value = value };
        return self._bt.put(keyPair);
    }

    /// Removes a key from the bucket.
    /// If the key does not exist then nothing is done and a nil error is returned.
    /// Returns an error if the bucket was created from a read-only transaction.
    /// TODO: add bool return indicate the key is deleted or not.
    pub fn delete(self: *Self, key: []const u8) Error!void {
        return self._bt.delete(key);
    }
};

/// A transaction is a read-write managed transaction.
pub const Transaction = struct {
    allocator: ?std.mem.Allocator,
    _tx: tx.TX,

    /// Writes all changes to disk and updates the meta page.
    /// Returns an error if a disk write error occurs, or if commit is
    /// called on a ready-only transaction.
    pub fn commit(self: *Transaction) Error!void {
        if (self.allocator) |allocator| {
            defer allocator.destroy(self);
        }
        try self._tx.commitAndDestroy();
    }

    /// Rolls back the transaction and destroys the transaction.
    pub fn rollback(self: *Transaction) void {
        if (self.allocator) |allocator| {
            allocator.destroy(self);
        }
        try self._tx.rollbackAndDestroy();
    }

    /// Retrieves a bucket any name.
    /// Returns null if the bucekt does not exist.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn bucket(self: *Transaction, name: []const u8) ?Bucket {
        if (self._tx.getBucket(name)) |bt| {
            return .{ ._bt = bt };
        }
        return null;
    }

    /// Creates a new bucket.
    /// Returns an error if the bucket already exists, if th bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucket(self: *Transaction, name: []const u8) Error!Bucket {
        if (self._tx.createBucket(name)) |bt| {
            return .{ ._bt = bt };
        }
        return null;
    }

    /// Creates a new bucket if the bucket if it doesn't already exist.
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn createBucketIfNotExists(self: *Transaction, name: []const u8) Error!Bucket {
        if (self._tx.createBucketIfNotExists(name)) |bt| {
            return .{ ._bt = bt };
        }
        return null;
    }

    /// Deletes a bucket.
    /// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
    pub fn deleteBucket(self: *Transaction, name: []const u8) Error!void {
        return self._tx.deleteBucket(name);
    }

    /// Returns the database that the transaction is associated with.
    pub fn database(self: *Transaction) Database {
        return Database{ ._db = self._tx.db };
    }

    /// Returns the ID of the transaction.
    pub fn id(self: *const Transaction) u64 {
        return self._tx.getID();
    }

    /// Returns the size of the transaction.
    pub fn size(self: *const Transaction) usize {
        return self._tx.getSize();
    }

    /// Returns true if the transaction is writable.
    pub fn writable(self: *const Transaction) bool {
        return self._tx.writable;
    }

    /// Returns the stats of the transaction.
    pub fn stats(self: *const Transaction) Stats {
        return self._tx.getStats();
    }

    /// Check performs several consistency checks on the database for this transaction.
    /// An error is returned if any inconsistency is found.
    ///
    /// It can be safely run concurrently on a writable transaction. However, this
    /// incurs a hight cost for large databases and databases with a lot of subbuckets.
    /// because of caching. This overhead can be removed if running on a read-only
    /// transaction. however, this is not safe to execute other writer-transactions at
    /// the same time.
    pub fn check(self: *Transaction) Error!void {
        return self._tx.check();
    }

    // copy writes the entire database to a writer.
    // This function exists for backwards compatibility.
    //
    // Deprecated; Use WriteTo() instead.
    pub fn copy(self: *Transaction) Error!void {
        return self._tx.copy();
    }

    /// Writes the entire database to a writer.
    pub fn writeTo(self: *Transaction, writer: anytype) Error!usize {
        return self._tx.writeToAnyWriter(writer);
    }

    /// Returns a reference to the page with a given id.
    /// If page has been written to then a temporary buffered page is returned.
    pub fn page(self: *Transaction, _id: u64) !?PageInfo {
        return self._tx.getPageInfo(_id);
    }

    /// Adds a handler function to be executed after the transaction successfully commits.
    pub fn onCommit(self: *Transaction, onCtx: *anyopaque, f: fn (
        ?*anyopaque,
        *Transaction,
    ) void) void {
        return self._tx.onCommit(onCtx, struct {
            fn exec(_: void, trans: *tx.TX) void {
                const _trans = Transaction{ .allocator = null, ._tx = trans };
                return f(onCtx, &_trans);
            }
        }.exec);
    }

    /// Creates a cursor assosicated with the root bucket.
    pub fn cursor(self: *Transaction) Cursor {
        return self._tx.cursor();
    }
};

/// A database is the main entry point for interacting with BoltDB.
pub const Database = struct {
    _db: *DB,
    const Self = @This();
    /// Creates and opens a database at the given path.
    /// If the file does not exist then it will be created automatically.
    /// Passing in null options will cause Bolt to open the database with the default options.
    pub fn open(allocator: std.mem.Allocator, filePath: []const u8, fileMode: ?std.fs.File.Mode, options: consts.Options) !*Self {
        const _db = try DB.open(allocator, filePath, fileMode, options);
        return .{ ._db = _db };
    }

    // Begin starts a new transaction.
    // Multiple read-only transactions can be used concurrently but only one write transaction can be used at a time. Starting multiple write transactions
    // will cause the calls to back and be serialized until the current write transaction finishes.
    //
    // Transactions should not be dependent on the one another. Opening a read
    // transaction and a write transaction in the same goroutine can cause the
    // writer to deadlock because the databases periodically needs to re-map itself
    // as it grows and it cannot do that while a read transaction is open.
    //
    // If a long running read transaction (for example, a snapshot transaction) is
    // needed, you might want to send DB.initialMmapSize to a larger enough value to avoid potential blocking of write transaction.
    //
    // *IMPORTANT*: You must close read-only transactions after you are finished or else the database will not reclaim old pages.
    pub fn begin(self: *Self, writable: bool) Error!*Transaction {
        if (writable) {
            const _tx = try self._db.beginRWTx();
            var trans = self._db.allocator.create(Transaction) catch unreachable;
            trans._tx = _tx;
            trans.allocator = self._db.allocator;
            return trans;
        } else {
            const _tx = try self._db.beginTx();
            var trans = self._db.allocator.create(Transaction) catch unreachable;
            trans._tx = _tx;
            trans.allocator = self._db.allocator;
            return trans;
        }
    }

    /// Executes a function within the context of a read-write managed transaction.
    /// If no error is returned from the function then the transaction is committed.
    /// If an error is returned then the entire transaction is rolled back.
    /// Any error that is returned from the function or returned from the commit is
    /// returned from the update() method.
    ///
    /// Attempting to manually commit or rollback within the function will cause a panic.
    pub fn update(self: *Self, execFn: fn (self: *Transaction) Error!void) Error!void {
        return self._db.update(struct {
            fn exec(_: void, trans: *tx.TX) Error!void {
                const _trans = Transaction{ .allocator = null, ._tx = trans };
                return execFn(&_trans._tx);
            }
        }.exec);
    }

    /// Executes a function within the context of a read-write managed transaction.
    pub fn updateWithContext(self: *Self, context: anytype, execFn: fn (ctx: @TypeOf(context), self: *Transaction) Error!void) Error!void {
        return self._db.updateWithContext(context, struct {
            fn exec(_: void, trans: *tx.TX) Error!void {
                const _trans = Transaction{ .allocator = null, ._tx = trans };
                return execFn(context, &_trans);
            }
        }.exec);
    }

    /// Executes a function within the context of a managed read-only transaction.
    /// Any error that is returned from the function is returned from the view() method.
    ///
    /// Attempting to manually rollback within the function will cause a panic.
    pub fn view(self: *Self, func: fn (self: *Transaction) Error!void) Error!void {
        return self.viewWithContext({}, struct {
            fn exec(_: void, trx: *tx.TX) Error!void {
                const _trans = Transaction{ .allocator = null, ._tx = trx };
                return func(&_trans);
            }
        }.exec);
    }

    /// Executes a function within the context of a managed read-only transaction.
    pub fn viewWithContext(self: *Self, context: anytype, func: fn (ctx: @TypeOf(context), self: *Transaction) Error!void) Error!void {
        return self._db.viewWithContext(context, struct {
            fn exec(_: void, trx: *tx.TX) Error!void {
                const _trans = Transaction{ .allocator = null, ._tx = trx };
                return func(context, &_trans);
            }
        }.exec);
    }
};

/// Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
/// Cursors see nested buckets with value == nil.
/// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
///
/// Keys and values returned from the cursor are only valid for the life of the transaction.
///
/// Changing data while traversing with a cursor may cause it to be invalidated
/// and return unexpected keys and/or values. You must reposition your cursor
/// after mutating data.
pub const Cursor = struct {
    _cursor: *tx.Cursor,
    const Self = @This();

    pub const KeyPair = consts.KeyPair;

    /// Deinitialize the cursor.
    pub fn deinit(self: *Self) void {
        self._cursor.deinit();
    }

    /// Returns the bucket that this cursor was created from.
    pub fn bucket(self: *Self) Bucket {
        return .{ ._bt = self._cursor.bucket() };
    }

    /// Moves the cursor to the first item in the bucket and returns its key and value.
    /// If the bucket is empty then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction
    pub fn first(self: *Self) KeyPair {
        return self._cursor.first();
    }

    /// Moves the cursor to the next item in the bucket and returns its key and value.
    /// If the cursor is at the end of the bucket then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn next(self: *Self) KeyPair {
        return self._cursor.next();
    }

    /// Moves the cursor to the last item in the bucket and returns its key and value.
    /// If the bucket is empty then a nil key and value are returned.
    pub fn last(self: *Self) KeyPair {
        return self._cursor.last();
    }

    /// Moves the cursor to the previous item in the bucket and returns its key and value.
    /// If the cursor is at the beginning of the bucket then a nil key and value are returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn prev(self: *Self) KeyPair {
        return self._cursor.prev();
    }

    /// Removes the current key/value under the cursor from the bucket.
    /// Delete fails if current key/value is a bucket or if the transaction is not writable.
    pub fn delete(self: *Self) Error!void {
        return self._cursor.delete();
    }

    /// Moves the cursor to a given key and returns it.
    /// If the key does not exist then the next key is used. If no keys
    /// follow, a nil key is returned.
    /// The returned key and value are only valid for the life of the transaction.
    pub fn seek(self: *Self, key: []const u8) KeyPair {
        return self._cursor.seek(key);
    }
};
