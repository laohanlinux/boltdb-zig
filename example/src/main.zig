//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");
const db = @import("boltdb");
const Error = db.Error;
pub const log_level: std.log.Level = .debug;

pub fn main() !void {
    std.testing.log_level = .debug;
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    var database = try db.Database.open(allocator, "boltdb.tmp", null, db.defaultOptions);
    defer database.close() catch unreachable;
    // create a bucket
    try struct {
        fn exec(_database: *db.Database) db.Error!void {
            var trans = try _database.begin(true);
            defer trans.commit() catch unreachable;
            var bucket = try trans.createBucketIfNotExists("user");
            try bucket.put("hello", "word");
        }
    }.exec(&database);

    // Get a bucket
    try struct {
        fn exec(_database: *db.Database) db.Error!void {
            var trans = try _database.begin(false);
            defer trans.rollback() catch unreachable;
            var bucket = trans.bucket("user").?;
            const value = bucket.get("hello").?;
            std.log.info("hello value is {s}", .{value});
        }
    }.exec(&database);

    // exec update
    {
        try database.update(struct {
            fn exec(trans: *db.Transaction) db.Error!void {
                var bucket = try trans.createBucketIfNotExists("user");
                try bucket.put("baz", "bar");
                const stats = trans.stats();
                std.log.info("transaction's stats: {any}", .{stats});
            }
        }.exec);

        try database.view(struct {
            fn view(trans: *db.Transaction) db.Error!void {
                var bucket = trans.bucket("user").?;
                const value = bucket.get("baz").?;
                std.log.info("baz value is {s}", .{value});
            }
        }.view);

        try database.viewWithContext({}, struct {
            fn view(_: void, trans: *db.Transaction) db.Error!void {
                var bucket = trans.bucket("user").?;
                const value = bucket.get("baz").?;
                std.log.info("baz value is {s}", .{value});
            }
        }.view);
    }

    // iterator
    {
        try struct {
            fn exec(_database: *db.Database) db.Error!void {
                var trans = try _database.begin(false);
                defer trans.rollback() catch unreachable;
                var cursor = trans.cursor();
                defer cursor.deinit();
                var keyPair = cursor.first();
                while (!keyPair.isNotFound()) {
                    if (keyPair.isBucket()) {
                        std.log.info("iterator DB: this is a bucket: {s}", .{keyPair.key.?});
                    } else {
                        std.log.info("iterator DB: key: {s}, value: {s}", .{ keyPair.key.?, keyPair.value.? });
                    }
                    keyPair = cursor.next();
                }
            }
        }.exec(&database);
    }
    {
        try struct {
            fn exec(_database: *db.Database) db.Error!void {
                var trans = try _database.begin(true);
                defer trans.commit() catch unreachable;
                var bucket = trans.bucket("user").?;
                std.log.info("Create a new bucket: {s}", .{"address"});
                var newBucket = try bucket.createBucketIfNotExists("date");
                std.log.info("Create a new bucket: {s}", .{"date"});
                var _allocator = std.heap.ArenaAllocator.init(_database.allocator());
                defer _allocator.deinit();
                const onceAllocator = _allocator.allocator();
                const value = std.fmt.allocPrint(onceAllocator, "{d}", .{std.time.timestamp()}) catch unreachable;
                try newBucket.put("laos", "Deloin");
                var cursor = bucket.cursor();
                defer cursor.deinit();
                var keyPair = cursor.first();
                while (!keyPair.isNotFound()) {
                    if (keyPair.isBucket()) {
                        std.log.info("iterator Bucket: this is a bucket: {s}", .{keyPair.key.?});
                    } else {
                        std.log.info("iterator Bucket: key: {s}, value: {s}", .{ keyPair.key.?, keyPair.value.? });
                    }
                    keyPair = cursor.next();
                }

                try bucket.put("dol", value);
                keyPair = cursor.seek("dol");
                if (keyPair.isNotFound()) {
                    std.log.info("not found key: {s}", .{"dol"});
                } else {
                    try cursor.delete();
                    std.log.info("delete key: {s}, value: {s}", .{ keyPair.key.?, keyPair.value.? });
                }
                const lastKeyPair = cursor.last();
                std.log.info("last key: {s}, value: {s}", .{ lastKeyPair.key.?, lastKeyPair.value.? });
                const prevKeyPair = cursor.prev();
                std.log.info("prev key: {s}", .{prevKeyPair.key.?});
                const cursorBucket = cursor.bucket();
                std.log.info("cursor's bucket: {any}", .{cursorBucket.stats()});

                try bucket.setSequence(100);
                const seq = try bucket.nextSequence();
                std.log.info("seq: {d}", .{seq});
                const root = bucket.root();
                std.log.info("root: {d}", .{root});
                const stats = trans.stats();
                std.log.info("transaction's stats: {any}", .{stats});
                const bucket_stats = bucket.stats();
                std.log.info("bucket's stats: {any}", .{bucket_stats});
                const writable = bucket.writable();
                std.log.info("bucket's writable: {}", .{writable});
                for (0..100) |i| {
                    try newBucket.put(std.fmt.allocPrint(onceAllocator, "{d}", .{i}) catch unreachable, "value");
                }

                std.log.info("Bucket forEach:", .{});
                try bucket.forEach(struct {
                    fn exec(_: *const db.Bucket, key: []const u8, _value: ?[]const u8) db.Error!void {
                        if (_value == null) {
                            std.log.info("this is a bucket, bucket name: {s}", .{key});
                        } else {
                            std.log.info("key: {s}, value: {s}", .{ key, _value.? });
                        }
                    }
                }.exec);

                std.log.info("Bucket forEach with Context", .{});
                var forEachCount: usize = 0;
                try bucket.forEachWithContext(&forEachCount, struct {
                    fn exec(ctxRef: *usize, _: *const db.Bucket, key: []const u8, _value: ?[]const u8) db.Error!void {
                        ctxRef.* += 1;
                        if (_value == null) {
                            std.log.info("this is a bucket, bucket name: {s}", .{key});
                        } else {
                            std.log.info("key: {s}, value: {s}", .{ key, _value.? });
                        }
                    }
                }.exec);
                std.log.info("forEachCount: {d}", .{forEachCount});
            }
        }.exec(&database);
    }

    {
        const path = database.path();
        std.log.info("database's path: {s}", .{path});
        const str = database.string(allocator);
        defer allocator.free(str);
        std.log.info("database's string: {s}", .{str});
        const isReadOnly = database.isReadOnly();
        std.log.info("database's isReadOnly: {}", .{isReadOnly});
        try database.sync();
    }
}
