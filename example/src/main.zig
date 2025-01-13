//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");
const db = @import("boltdb");
const Error = db.Error;
pub const log_level: std.log.Level = .debug;
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    var database = try db.Database.open(allocator, "boltdb.tmp", null, db.defaultOptions);
    try database.close();

    // create a bucket
    {
        var tx = try database.begin(true);
        var bucket = try tx.createBucket("user");
        try bucket.put("hello", "word");
        try tx.commit();
    }

    // Get a bucket
    {
        var tx = try database.begin(false);
        var bucket = tx.bucket("user").?;
        const value = bucket.get("hello").?;
        std.log.info("hello value is {s}", .{value});
        try tx.rollback();
    }

    {
        try database.update(struct {
            fn exec(trans: *db.Transaction) Error!void {
                var bucket = try trans.createBucketIfNotExists("user");
                try bucket.put("baz", "bar");
            }
        }.exec);

        try database.view(struct {
            fn view(trans: *db.Transaction) Error!void {
                const bucket = trans.bucket("user").?;
                const value = bucket.get("baz").?;
                std.log.info("baz value is {}\n", .{value});
            }
        }.view);
    }
}
