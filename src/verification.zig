//! Post-fsync verification and LSE recovery logic.

const std = @import("std");
const record = @import("record.zig");
const Record = record.Record;

pub const VerifyResult = union(enum) {
    success: Record,
    checksum_mismatch: struct { expected: u32, actual: u32 },
    io_error: i32,
};

pub const FileId = enum { primary, secondary };

/// Verify record at buffer, checking magic, sequence, and checksum.
pub fn verify_record(buffer: []const u8, expected_sequence: u64) VerifyResult {
    if (buffer.len < record.HEADER_SIZE) {
        return .{ .io_error = -1 };
    }

    // Try to deserialize
    const deserialized = Record.deserialize(buffer) catch |err| {
        return switch (err) {
            error.BufferTooSmall, error.InvalidLength => .{ .io_error = -1 },
            error.InvalidMagic, error.InvalidSequence => .{ .io_error = -1 },
        };
    };

    // Verify sequence matches
    if (deserialized.header.sequence != expected_sequence) {
        return .{ .io_error = -1 };
    }

    // Verify checksum
    if (!deserialized.verify_checksum()) {
        const expected_crc = Record.calculate_checksum(
            deserialized.header.sequence,
            deserialized.header.length,
            deserialized.payload,
        );
        return .{
            .checksum_mismatch = .{
                .expected = expected_crc,
                .actual = deserialized.header.checksum,
            },
        };
    }

    return .{ .success = deserialized };
}

// Tests
test "verify_record detects valid record" {
    const sequence = 42;
    const payload = "test data";

    const buffer = try std.testing.allocator.alloc(u8, 512);
    defer std.testing.allocator.free(buffer);

    // Serialize valid record
    const serialized = try Record.serialize(std.testing.allocator, sequence, payload);
    defer std.testing.allocator.free(serialized);

    @memcpy(buffer, serialized);

    const result = verify_record(buffer, sequence);
    try std.testing.expect(result == .success);
    try std.testing.expectEqualSlices(u8, result.success.payload, payload);
}

test "verify_record detects checksum mismatch" {
    const sequence = 42;
    const payload = "test data";

    var buffer = try std.testing.allocator.alloc(u8, 512);
    defer std.testing.allocator.free(buffer);

    // Serialize then corrupt checksum
    const serialized = try Record.serialize(std.testing.allocator, sequence, payload);
    defer std.testing.allocator.free(serialized);

    @memcpy(buffer, serialized);
    std.mem.writeInt(u32, buffer[16..20], 0xDEADBEEF, .little);

    const result = verify_record(buffer, sequence);
    try std.testing.expect(result == .checksum_mismatch);
}

test "verify_record detects wrong sequence" {
    const sequence = 42;
    const payload = "test data";

    const buffer = try std.testing.allocator.alloc(u8, 512);
    defer std.testing.allocator.free(buffer);

    // Serialize with one sequence, verify with another
    const serialized = try Record.serialize(std.testing.allocator, sequence, payload);
    defer std.testing.allocator.free(serialized);

    @memcpy(buffer, serialized);

    const result = verify_record(buffer, 999); // Different sequence
    try std.testing.expect(result == .io_error);
}

test "verify_record handles too-small buffer" {
    const result = verify_record(&[_]u8{}, 42);
    try std.testing.expect(result == .io_error);
}

test "verify_record detects invalid magic" {
    const buffer = try std.testing.allocator.alloc(u8, 512);
    defer std.testing.allocator.free(buffer);

    @memset(buffer, 0);
    std.mem.writeInt(u32, buffer[0..4], 0xDEADBEEF, .little); // Wrong magic

    const result = verify_record(buffer, 42);
    try std.testing.expect(result == .io_error);
}
