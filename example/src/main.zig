//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.
const std = @import("std");
const db = @import("boltdb");
pub const log_level: std.log.Level = .debug;
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}).init;
    const allocator = gpa.allocator();
    var database = try db.Database.open(allocator, "boltdb.tmp", null, db.defaultOptions);
    defer database.close();
    std.debug.print("{any}\n", .{db.defaultOptions});
}
