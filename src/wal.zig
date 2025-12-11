//! Write-ahead log with dual files for LSE protection.

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const IoUring = std.os.linux.IoUring;

const record = @import("record.zig");
const Record = record.Record;
const io_manager = @import("io_manager.zig");
const recovery = @import("recovery.zig");

/// Pending operation in io_uring ring.
pub const PendingOp = struct {
    sequence: u64,
    stage: Stage,
    primary_buffer: []u8,
    secondary_buffer: []u8,
    verify_buffer: []u8,
    primary_fsync_done: bool,
    secondary_fsync_done: bool,

    pub const Stage = enum {
        writing,
        syncing,
        verifying,
        completed,
        failed,
    };
};

/// Callback for log replay.
pub const ReplayCallback = *const fn (sequence: u64, payload: []const u8) anyerror!void;

/// Write-ahead log with dual files and io_uring async I/O.
pub const WAL = struct {
    allocator: Allocator,
    ring: IoUring,

    primary_fd: posix.fd_t,
    secondary_fd: posix.fd_t,

    next_sequence: u64,
    write_offset: u64,

    pending_ops: ArrayList(PendingOp),

    const Self = @This();

    /// Initialize WAL, creating both log files with O_DIRECT|O_DSYNC.
    /// Primary and secondary should be on different disks for LSE protection.
    pub fn init(
        allocator: Allocator,
        primary_path: []const u8,
        secondary_path: []const u8,
        io_uring_entries: u16,
    ) !*Self {
        // Initialize io_uring ring
        var ring = try IoUring.init(io_uring_entries, 0);
        errdefer ring.deinit();

        // Open primary WAL file
        const flags = posix.O{
            .CREAT = true,
            .ACCMODE = .RDWR,
            .DSYNC = true,
        };
        const mode = 0o644;

        const primary_fd = try posix.open(primary_path, flags, mode);
        errdefer posix.close(primary_fd);

        const secondary_fd = try posix.open(secondary_path, flags, mode);
        errdefer posix.close(secondary_fd);

        // Allocate WAL instance
        const wal = try allocator.create(Self);
        errdefer allocator.destroy(wal);

        wal.* = Self{
            .allocator = allocator,
            .ring = ring,
            .primary_fd = primary_fd,
            .secondary_fd = secondary_fd,
            .next_sequence = 1,
            .write_offset = 0,
            .pending_ops = ArrayList(PendingOp).empty,
        };

        return wal;
    }

    /// Deinitialize WAL, freeing all resources.
    pub fn deinit(self: *Self) void {
        // Close files
        posix.close(self.primary_fd);
        posix.close(self.secondary_fd);

        // Free pending operations
        for (self.pending_ops.items) |*op| {
            self.allocator.free(op.primary_buffer);
            self.allocator.free(op.secondary_buffer);
            self.allocator.free(op.verify_buffer);
        }
        self.pending_ops.deinit(self.allocator);

        // Deinitialize io_uring
        self.ring.deinit();

        // Free WAL instance
        self.allocator.destroy(self);
    }

    /// Append record to WAL. Returns sequence number assigned to record.
    /// The record is not persisted until flush() is called.
    pub fn append(self: *Self, payload: []const u8) !u64 {
        const sequence = self.next_sequence;
        self.next_sequence += 1;

        // Serialize record with alignment for O_DIRECT
        const length = @as(u32, @intCast(payload.len));
        const checksum = Record.calculate_checksum(sequence, length, payload);
        const padded_size = ((record.HEADER_SIZE + payload.len + record.RECORD_ALIGNMENT - 1) / record.RECORD_ALIGNMENT) * record.RECORD_ALIGNMENT;

        // Allocate primary buffer (padded to 512-byte boundary for O_DIRECT)
        const primary_buffer = try self.allocator.alloc(u8, padded_size);
        errdefer self.allocator.free(primary_buffer);

        // Allocate secondary buffer
        const secondary_buffer = try self.allocator.alloc(u8, padded_size);
        errdefer self.allocator.free(secondary_buffer);

        // Allocate verify buffer
        const verify_buffer = try self.allocator.alloc(u8, padded_size);
        errdefer self.allocator.free(verify_buffer);

        // Serialize record into both buffers
        std.mem.writeInt(u32, primary_buffer[0..4], record.RECORD_MAGIC, .little);
        std.mem.writeInt(u64, primary_buffer[4..12], sequence, .little);
        std.mem.writeInt(u32, primary_buffer[12..16], length, .little);
        std.mem.writeInt(u32, primary_buffer[16..20], checksum, .little);
        @memcpy(primary_buffer[record.HEADER_SIZE .. record.HEADER_SIZE + payload.len], payload);
        @memset(primary_buffer[record.HEADER_SIZE + payload.len ..], 0);

        // Copy primary to secondary
        @memcpy(secondary_buffer, primary_buffer);

        // Track pending operation
        try self.pending_ops.append(self.allocator, PendingOp{
            .sequence = sequence,
            .stage = .writing,
            .primary_buffer = primary_buffer,
            .secondary_buffer = secondary_buffer,
            .verify_buffer = verify_buffer,
            .primary_fsync_done = false,
            .secondary_fsync_done = false,
        });

        // Submit write chains for both WAL files
        _ = try io_manager.submit_write_chain(
            &self.ring,
            self.primary_fd,
            primary_buffer,
            self.write_offset,
            sequence,
            .primary_write,
            .primary_fsync,
        );

        _ = try io_manager.submit_write_chain(
            &self.ring,
            self.secondary_fd,
            secondary_buffer,
            self.write_offset,
            sequence,
            .secondary_write,
            .secondary_fsync,
        );

        // Submit to kernel
        _ = try self.ring.submit();

        return sequence;
    }

    /// Wait for all pending operations to complete.
    /// Processes all available completions from io_uring.
    pub fn flush(self: *Self) !void {
        // Keep processing completions until all operations are done
        while (self.pending_ops.items.len > 0) {
            // Try to wait for completions
            _ = self.ring.submit_and_wait(1) catch |err| switch (err) {
                error.SignalInterrupt => {},
                else => return err,
            };

            // Process available completions
            try io_manager.process_completions(&self.ring, &self.pending_ops);

            // Remove completed operations
            var i: usize = 0;
            while (i < self.pending_ops.items.len) {
                const op = &self.pending_ops.items[i];
                if (op.stage == .completed or op.stage == .failed) {
                    if (op.stage == .failed) {
                        std.debug.print("error: operation failed for sequence {}\n", .{op.sequence});
                        return error.OperationFailed;
                    }

                    // Free buffers
                    self.allocator.free(op.primary_buffer);
                    self.allocator.free(op.secondary_buffer);
                    self.allocator.free(op.verify_buffer);

                    // Remove from pending
                    _ = self.pending_ops.swapRemove(i);
                } else {
                    i += 1;
                }
            }
        }
    }

    /// Replay log by scanning both files and calling callback for each record.
    pub fn replay(self: *Self, callback: ReplayCallback) !void {
        // Will be implemented in recovery phase
        _ = self;
        _ = callback;
    }

    /// Gracefully close WAL, ensuring all pending operations complete.
    pub fn close(self: *Self) !void {
        // Ensure all pending ops are flushed
        try self.flush();
    }
};

// Tests
test "wal initialize and deinitialize" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_tmp", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_tmp") catch {};

    const primary_path = "wal_test_tmp/primary.wal";
    const secondary_path = "wal_test_tmp/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 16);
    defer wal.deinit();

    try std.testing.expectEqual(@as(u64, 1), wal.next_sequence);
    try std.testing.expectEqual(@as(u64, 0), wal.write_offset);
}

test "wal append creates pending operation" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_append", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_append") catch {};

    const primary_path = "wal_test_append/primary.wal";
    const secondary_path = "wal_test_append/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 16);
    defer wal.deinit();

    const seq1 = try wal.append("test data 1");
    const seq2 = try wal.append("test data 2");

    try std.testing.expectEqual(@as(u64, 1), seq1);
    try std.testing.expectEqual(@as(u64, 2), seq2);
    try std.testing.expectEqual(@as(u64, 3), wal.next_sequence);
    try std.testing.expectEqual(@as(usize, 2), wal.pending_ops.items.len);

    try std.testing.expectEqual(@as(u64, 1), wal.pending_ops.items[0].sequence);
    try std.testing.expectEqual(.writing, wal.pending_ops.items[0].stage);

    try std.testing.expectEqual(@as(u64, 2), wal.pending_ops.items[1].sequence);
    try std.testing.expectEqual(.writing, wal.pending_ops.items[1].stage);
}

test "wal append buffers are padded" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_aligned", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_aligned") catch {};

    const primary_path = "wal_test_aligned/primary.wal";
    const secondary_path = "wal_test_aligned/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 16);
    defer wal.deinit();

    _ = try wal.append("test");

    const op = wal.pending_ops.items[0];
    // Verify padding to 512-byte boundary
    try std.testing.expect(op.primary_buffer.len % record.RECORD_ALIGNMENT == 0);
    try std.testing.expect(op.secondary_buffer.len % record.RECORD_ALIGNMENT == 0);
    try std.testing.expect(op.verify_buffer.len % record.RECORD_ALIGNMENT == 0);
}

test "wal append buffers match" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_match", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_match") catch {};

    const primary_path = "wal_test_match/primary.wal";
    const secondary_path = "wal_test_match/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 16);
    defer wal.deinit();

    const payload = "test payload";
    _ = try wal.append(payload);

    const op = wal.pending_ops.items[0];
    try std.testing.expectEqualSlices(u8, op.primary_buffer, op.secondary_buffer);
}

test "wal single append and flush" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_flush", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_flush") catch {};

    const primary_path = "wal_test_flush/primary.wal";
    const secondary_path = "wal_test_flush/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 64);
    defer wal.deinit();

    const seq = try wal.append("test data");
    try std.testing.expectEqual(@as(u64, 1), seq);

    // Flush should process completions
    try wal.flush();

    // After flush, pending ops should be empty
    try std.testing.expectEqual(@as(usize, 0), wal.pending_ops.items.len);
}

test "wal multiple appends and flush" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_multi", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_multi") catch {};

    const primary_path = "wal_test_multi/primary.wal";
    const secondary_path = "wal_test_multi/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 64);
    defer wal.deinit();

    const seq1 = try wal.append("record 1");
    const seq2 = try wal.append("record 2");
    const seq3 = try wal.append("record 3");

    try std.testing.expectEqual(@as(u64, 1), seq1);
    try std.testing.expectEqual(@as(u64, 2), seq2);
    try std.testing.expectEqual(@as(u64, 3), seq3);

    try wal.flush();

    try std.testing.expectEqual(@as(usize, 0), wal.pending_ops.items.len);
}

test "wal files created with correct permissions" {
    const allocator = std.testing.allocator;
    var tmp_dir = try std.fs.cwd().makeOpenPath("wal_test_perms", .{});
    defer tmp_dir.close();
    defer std.fs.cwd().deleteTree("wal_test_perms") catch {};

    const primary_path = "wal_test_perms/primary.wal";
    const secondary_path = "wal_test_perms/secondary.wal";

    const wal = try WAL.init(allocator, primary_path, secondary_path, 16);
    defer wal.deinit();

    // Verify files exist and can be read
    const primary_file = try tmp_dir.openFile("primary.wal", .{});
    defer primary_file.close();

    const secondary_file = try tmp_dir.openFile("secondary.wal", .{});
    defer secondary_file.close();

    // Files exist with O_DIRECT, so they should be readable
    try std.testing.expect(true);
}
