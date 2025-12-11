//! io_uring operation management with linked write→fsync chains.

const std = @import("std");
const posix = std.posix;
const IoUring = std.os.linux.IoUring;

const wal = @import("wal.zig");
const PendingOp = wal.PendingOp;

pub const UserDataTag = enum(u8) {
    primary_write = 0,
    primary_fsync = 1,
    primary_verify = 2,
    secondary_write = 3,
    secondary_fsync = 4,
    secondary_verify = 5,
};

/// Encode sequence (56 bits) and tag (8 bits) into user_data.
/// Bits 0-7: tag, Bits 8-63: sequence
pub fn encode_user_data(sequence: u64, tag: UserDataTag) u64 {
    return (sequence << 8) | @intFromEnum(tag);
}

/// Decode user_data back to sequence and tag.
pub fn decode_user_data(user_data: u64) struct { sequence: u64, tag: UserDataTag } {
    const tag_val = @as(u8, @intCast(user_data & 0xFF));
    const sequence_val = user_data >> 8;
    return .{
        .sequence = sequence_val,
        .tag = @enumFromInt(tag_val),
    };
}

/// Submit write operation linked to fsync.
/// The fsync SQE will only complete after the write completes.
/// Returns true if both SQEs were submitted, false if ring is full.
pub fn submit_write_chain(
    ring: *IoUring,
    fd: posix.fd_t,
    buffer: []const u8,
    offset: u64,
    sequence: u64,
    write_tag: UserDataTag,
    fsync_tag: UserDataTag,
) !bool {
    // Get write SQE
    var write_sqe = try ring.get_sqe();
    write_sqe.prep_write(fd, buffer, offset);
    write_sqe.user_data = encode_user_data(sequence, write_tag);
    write_sqe.flags |= std.os.linux.IOSQE_IO_LINK; // Link to next SQE

    // Get fsync SQE
    var fsync_sqe = try ring.get_sqe();
    fsync_sqe.prep_fsync(fd, 0); // Full file fsync
    fsync_sqe.user_data = encode_user_data(sequence, fsync_tag);

    return true;
}

/// Submit read verification operation.
pub fn submit_verify_read(
    ring: *IoUring,
    fd: posix.fd_t,
    buffer: []u8,
    offset: u64,
    length: u32,
    sequence: u64,
    tag: UserDataTag,
) !bool {
    var read_sqe = try ring.get_sqe();
    read_sqe.prep_read(fd, buffer[0..length], offset);
    read_sqe.user_data = encode_user_data(sequence, tag);
    return true;
}

/// Process all available completions from the ring.
/// Updates pending operations with results and stages them appropriately.
pub fn process_completions(
    ring: *IoUring,
    pending_ops: *std.ArrayList(PendingOp),
) !void {
    var cq_ready = ring.cq_ready();

    while (cq_ready > 0) : (cq_ready -= 1) {
        const cqe = try ring.copy_cqe();
        const decoded = decode_user_data(cqe.user_data);

        // Find pending operation for this sequence
        var op_index: ?usize = null;
        for (pending_ops.items, 0..) |*op, i| {
            if (op.sequence == decoded.sequence) {
                op_index = i;
                break;
            }
        }

        if (op_index == null) {
            // This shouldn't happen in normal operation
            std.debug.print("error: received CQE for unknown sequence {}\n", .{decoded.sequence});
            return error.UnknownSequence;
        }

        const i = op_index.?;
        var op = &pending_ops.items[i];

        if (cqe.res < 0) {
            // I/O error
            const errno = @as(i32, @intCast(-cqe.res));
            std.debug.print("error: I/O failed for sequence {} tag {}: errno {}\n", .{ decoded.sequence, decoded.tag, errno });
            op.stage = .failed;
            continue;
        }

        // Update stage based on which operation completed
        switch (decoded.tag) {
            .primary_write, .secondary_write => {
                // Write completed, fsync will follow via linked SQE
                // No stage change needed - still in writing phase
            },
            .primary_fsync => {
                // Primary fsync completed
                op.primary_fsync_done = true;
                // Mark completed only after both fsyncs finish
                if (op.secondary_fsync_done) {
                    op.stage = .completed;
                }
            },
            .secondary_fsync => {
                // Secondary fsync completed
                op.secondary_fsync_done = true;
                // Mark completed only after both fsyncs finish
                if (op.primary_fsync_done) {
                    op.stage = .completed;
                }
            },
            .primary_verify, .secondary_verify => {
                // Verify read completed (if used)
                op.stage = .completed;
            },
        }
    }
}

// Tests
test "user data encoding/decoding round-trip" {
    const sequence = 123456;
    const tag = UserDataTag.primary_write;

    const encoded = encode_user_data(sequence, tag);
    const decoded = decode_user_data(encoded);

    try std.testing.expectEqual(sequence, decoded.sequence);
    try std.testing.expectEqual(tag, decoded.tag);
}

test "user data preserves all sequences" {
    const sequences = [_]u64{ 1, 100, 1000, 0xFFFFFFFFFFFFFF }; // Max 56-bit

    for (sequences) |seq| {
        const encoded = encode_user_data(seq, .primary_fsync);
        const decoded = decode_user_data(encoded);
        try std.testing.expectEqual(seq, decoded.sequence);
    }
}

test "user data distinguishes all tags" {
    const tags = [_]UserDataTag{
        .primary_write,
        .primary_fsync,
        .primary_verify,
        .secondary_write,
        .secondary_fsync,
        .secondary_verify,
    };

    for (tags) |tag| {
        const encoded = encode_user_data(42, tag);
        const decoded = decode_user_data(encoded);
        try std.testing.expectEqual(tag, decoded.tag);
    }
}

test "user data encoding produces distinct values" {
    const ud1 = encode_user_data(1, .primary_write);
    const ud2 = encode_user_data(1, .primary_fsync);
    const ud3 = encode_user_data(2, .primary_write);

    try std.testing.expect(ud1 != ud2);
    try std.testing.expect(ud1 != ud3);
    try std.testing.expect(ud2 != ud3);
}
