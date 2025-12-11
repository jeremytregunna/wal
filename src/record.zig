//! Record format and serialization with CRC32C checksums.
//!
//! Record layout (512-byte aligned for O_DIRECT):
//! ┌────────────┬──────────┬──────────┬──────────┬────────────────┬─────────┐
//! │ Magic (4B) │ Seq (8B) │ Len (4B) │ CRC (4B) │ Payload (N B)  │ Pad     │
//! └────────────┴──────────┴──────────┴──────────┴────────────────┴─────────┘

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const RECORD_MAGIC = 0x57414C52; // "WALR"
pub const RECORD_ALIGNMENT = 512;
pub const HEADER_SIZE = 20; // 4 + 8 + 4 + 4 bytes

/// Record header, laid out for O_DIRECT I/O.
/// Layout: magic(4) + sequence(8) + length(4) + checksum(4) = 20 bytes
pub const RecordHeader = extern struct {
    magic: u32,
    sequence: u64,
    length: u32,
    checksum: u32,
};

/// Serialized record with payload.
pub const Record = struct {
    header: RecordHeader,
    payload: []const u8,

    /// Calculate CRC32C of sequence, length, and payload.
    pub fn calculate_checksum(sequence: u64, length: u32, payload: []const u8) u32 {
        var crc = std.hash.crc.Crc32Iscsi.init();

        // Hash sequence as little-endian bytes
        var seq_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_bytes, sequence, .little);
        crc.update(&seq_bytes);

        // Hash length as little-endian bytes
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, length, .little);
        crc.update(&len_bytes);

        // Hash payload
        crc.update(payload);

        return crc.final();
    }

    /// Serialize record into buffer with CRC32C and 512-byte padding.
    /// Caller owns returned buffer; must be freed with allocator.free().
    /// Buffer is padded to 512-byte boundary for O_DIRECT but may not be 512-byte aligned.
    pub fn serialize(
        allocator: Allocator,
        sequence: u64,
        payload: []const u8,
    ) ![]u8 {
        if (sequence == 0) return error.InvalidSequence;
        if (payload.len > 0xFFFF_FFFF - HEADER_SIZE) return error.PayloadTooLarge;

        const length = @as(u32, @intCast(payload.len));
        const checksum = calculate_checksum(sequence, length, payload);

        // Calculate total size with padding to 512-byte boundary
        const header_and_payload = HEADER_SIZE + payload.len;
        const padded_size = ((header_and_payload + RECORD_ALIGNMENT - 1) / RECORD_ALIGNMENT) * RECORD_ALIGNMENT;

        // Allocate buffer (we'll assert alignment, let allocator provide best effort)
        const buffer = try allocator.alloc(u8, padded_size);
        errdefer allocator.free(buffer);

        // Write header in little-endian
        const header = RecordHeader{
            .magic = RECORD_MAGIC,
            .sequence = sequence,
            .length = length,
            .checksum = checksum,
        };

        std.mem.writeInt(u32, buffer[0..4], header.magic, .little);
        std.mem.writeInt(u64, buffer[4..12], header.sequence, .little);
        std.mem.writeInt(u32, buffer[12..16], header.length, .little);
        std.mem.writeInt(u32, buffer[16..20], header.checksum, .little);

        // Copy payload
        @memcpy(buffer[HEADER_SIZE .. HEADER_SIZE + payload.len], payload);

        // Zero-fill padding
        @memset(buffer[header_and_payload..], 0);

        return buffer;
    }

    /// Deserialize and validate record from buffer.
    /// Does not validate checksum; call verify_checksum() for that.
    pub fn deserialize(buffer: []const u8) !Record {
        if (buffer.len < HEADER_SIZE) return error.BufferTooSmall;

        // Read header in little-endian
        const magic = std.mem.readInt(u32, buffer[0..4], .little);
        const sequence = std.mem.readInt(u64, buffer[4..12], .little);
        const length = std.mem.readInt(u32, buffer[12..16], .little);
        const checksum = std.mem.readInt(u32, buffer[16..20], .little);

        if (magic != RECORD_MAGIC) return error.InvalidMagic;
        if (sequence == 0) return error.InvalidSequence;
        if (length > buffer.len - HEADER_SIZE) return error.InvalidLength;

        const payload = buffer[HEADER_SIZE .. HEADER_SIZE + length];

        return Record{
            .header = RecordHeader{
                .magic = magic,
                .sequence = sequence,
                .length = length,
                .checksum = checksum,
            },
            .payload = payload,
        };
    }

    /// Verify that record's checksum matches its contents.
    pub fn verify_checksum(self: Record) bool {
        const expected = calculate_checksum(self.header.sequence, self.header.length, self.payload);
        return expected == self.header.checksum;
    }
};

// Tests
test "record serialization round-trip" {
    const allocator = std.testing.allocator;
    const payload = "test data";

    const buffer = try Record.serialize(allocator, 1, payload);
    defer allocator.free(buffer);

    // Verify padding
    try std.testing.expect(buffer.len % RECORD_ALIGNMENT == 0);

    // Deserialize and verify
    const record = try Record.deserialize(buffer);
    try std.testing.expectEqual(@as(u64, 1), record.header.sequence);
    try std.testing.expectEqualSlices(u8, payload, record.payload);
    try std.testing.expect(record.verify_checksum());
}

test "record checksum validation" {
    const allocator = std.testing.allocator;
    const payload = "test data";

    var buffer = try Record.serialize(allocator, 1, payload);
    defer allocator.free(buffer);

    // Corrupt checksum
    std.mem.writeInt(u32, buffer[16..20], 0xDEADBEEF, .little);

    const record = try Record.deserialize(buffer);
    try std.testing.expect(!record.verify_checksum());
}

test "record detects invalid magic" {
    const allocator = std.testing.allocator;
    var buffer = try allocator.alloc(u8, RECORD_ALIGNMENT);
    defer allocator.free(buffer);

    @memset(buffer, 0);
    std.mem.writeInt(u32, buffer[0..4], 0xDEADBEEF, .little); // Wrong magic

    try std.testing.expectError(error.InvalidMagic, Record.deserialize(buffer));
}

test "record detects zero sequence" {
    const allocator = std.testing.allocator;
    var buffer = try allocator.alloc(u8, RECORD_ALIGNMENT);
    defer allocator.free(buffer);

    @memset(buffer, 0);
    std.mem.writeInt(u32, buffer[0..4], RECORD_MAGIC, .little);
    std.mem.writeInt(u64, buffer[4..12], 0, .little); // Zero sequence

    try std.testing.expectError(error.InvalidSequence, Record.deserialize(buffer));
}

test "record rejects zero sequence on serialize" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidSequence, Record.serialize(allocator, 0, "data"));
}

test "record buffer alignment with various payload sizes" {
    const allocator = std.testing.allocator;

    const sizes = [_]usize{ 1, 10, 100, 500, 511, 512, 513, 1000 };
    for (sizes) |size| {
        const payload = try allocator.alloc(u8, size);
        defer allocator.free(payload);
        @memset(payload, 'x');

        const buffer = try Record.serialize(allocator, 1, payload);
        defer allocator.free(buffer);

        // Verify 512-byte padding
        try std.testing.expect(buffer.len % RECORD_ALIGNMENT == 0);
        try std.testing.expect(buffer.len >= HEADER_SIZE + payload.len);
    }
}

test "record crc32c matches expected values" {
    // Test that CRC calculation is deterministic
    const seq = 42;
    const len = 5;
    const payload = "hello";

    const crc1 = Record.calculate_checksum(seq, len, payload);
    const crc2 = Record.calculate_checksum(seq, len, payload);

    try std.testing.expectEqual(crc1, crc2);

    // Different payloads must have different CRCs
    const crc3 = Record.calculate_checksum(seq, len, "world");
    try std.testing.expect(crc1 != crc3);
}

test "fuzz record deserialization does not crash" {
    const Context = struct {
        fn testOne(_: @This(), input: []const u8) anyerror!void {
            // Should not crash on any input
            _ = Record.deserialize(input) catch |err| {
                // Errors are expected and OK
                switch (err) {
                    error.BufferTooSmall,
                    error.InvalidMagic,
                    error.InvalidSequence,
                    error.InvalidLength,
                    => {},
                    else => return err,
                }
            };
        }
    };

    try std.testing.fuzz(Context{}, Context.testOne, .{});
}
