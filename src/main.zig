const std = @import("std");
const wal_lib = @import("wal");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create WAL with primary and secondary files
    const wal = try wal_lib.WAL.init(
        allocator,
        "/tmp/wal_primary.log",
        "/tmp/wal_secondary.log",
        256,
    );
    defer wal.deinit();

    std.debug.print("=== Write-Ahead Log Demo ===\n", .{});
    std.debug.print("Initialized WAL with io_uring for dual-file persistence\n", .{});

    // Append some records
    const seq1 = try wal.append("Hello, WAL!");
    std.debug.print("Appended record 1 with sequence {}\n", .{seq1});

    const seq2 = try wal.append("This is record 2 with more data");
    std.debug.print("Appended record 2 with sequence {}\n", .{seq2});

    const seq3 = try wal.append("Final record in batch");
    std.debug.print("Appended record 3 with sequence {}\n", .{seq3});

    // Flush to ensure all records are persisted with fsync
    std.debug.print("\nFlushing WAL (this may take a moment due to fsync latency)...\n", .{});
    try wal.flush();
    std.debug.print("All records successfully written to disk and verified\n", .{});

    std.debug.print("\nWAL Features Demonstrated:\n", .{});
    std.debug.print("  • io_uring async I/O with linked write→fsync operations\n", .{});
    std.debug.print("  • CRC32C checksums on every record\n", .{});
    std.debug.print("  • Dual WAL files for LSE (Latent Sector Error) protection\n", .{});
    std.debug.print("  • O_DIRECT + O_DSYNC for cache bypass and ordering\n", .{});
    std.debug.print("  • Post-fsync verification reads with checksum validation\n", .{});
}
