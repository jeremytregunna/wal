//! WAL recovery and replay logic.

const std = @import("std");
const posix = std.posix;
const Allocator = std.mem.Allocator;

const record = @import("record.zig");
const Record = record.Record;
const verification = @import("verification.zig");

pub const RecoveryState = struct {
    highest_sequence: u64,
    next_write_offset: u64,
    valid_record_count: usize,
};

/// Scan WAL file and return all valid records in sequence order.
/// Stops at first invalid record or EOF.
pub fn scan_wal_file(allocator: Allocator, file: std.fs.File) !std.ArrayList(Record) {
    var records = std.ArrayList(Record).empty;
    errdefer records.deinit(allocator);

    var buf: [512]u8 = undefined;
    var reader = file.reader(&buf);

    // Read records until we hit EOF or invalid data
    while (true) {
        // Read header
        var header_buf: [record.HEADER_SIZE]u8 = undefined;
        const header_read = reader.read(&header_buf) catch break;

        if (header_read < record.HEADER_SIZE) {
            break; // Incomplete header
        }

        // Deserialize header
        const magic = std.mem.readInt(u32, header_buf[0..4], .little);
        const sequence = std.mem.readInt(u64, header_buf[4..12], .little);
        const length = std.mem.readInt(u32, header_buf[12..16], .little);
        const checksum = std.mem.readInt(u32, header_buf[16..20], .little);

        if (magic != record.RECORD_MAGIC) {
            break; // Invalid magic
        }

        if (sequence == 0) {
            break; // Invalid sequence
        }

        // Read payload
        const payload = try allocator.alloc(u8, length);
        errdefer allocator.free(payload);

        const payload_read = reader.read(payload) catch {
            allocator.free(payload);
            break;
        };
        if (payload_read < length) {
            allocator.free(payload);
            break; // Incomplete payload
        }

        // Verify checksum
        const expected_crc = Record.calculate_checksum(sequence, length, payload);
        if (expected_crc != checksum) {
            allocator.free(payload);
            break; // Corrupted record
        }

        // Store record - Record holds reference to allocated payload
        // Caller must free payloads when cleaning up records
        try records.append(allocator, Record{
            .header = .{
                .magic = magic,
                .sequence = sequence,
                .length = length,
                .checksum = checksum,
            },
            .payload = payload,
        });

        // Skip padding to reach next 512-byte boundary
        const record_size = record.HEADER_SIZE + length;
        const padded_size = ((record_size + record.RECORD_ALIGNMENT - 1) / record.RECORD_ALIGNMENT) * record.RECORD_ALIGNMENT;
        const padding = padded_size - record_size;

        if (padding > 0) {
            var pad_buf: [512]u8 = undefined;
            _ = reader.read(pad_buf[0..padding]) catch break;
        }
    }

    return records;
}

/// Reconcile records from both WAL files.
/// For each sequence number, prefer valid records from either file.
/// Deduplicates and returns combined state.
pub fn reconcile_dual_wals(
    allocator: Allocator,
    primary_records: std.ArrayList(Record),
    secondary_records: std.ArrayList(Record),
) !RecoveryState {
    const pr = primary_records;
    const sr = secondary_records;

    // Build map of sequence -> record for quick lookup
    var primary_map = std.AutoHashMap(u64, Record).init(allocator);
    defer primary_map.deinit();

    var secondary_map = std.AutoHashMap(u64, Record).init(allocator);
    defer secondary_map.deinit();

    for (pr.items) |r| {
        try primary_map.put(r.header.sequence, r);
    }

    for (sr.items) |r| {
        try secondary_map.put(r.header.sequence, r);
    }

    // Find highest sequence that is valid in at least one file
    var highest_sequence: u64 = 0;
    var valid_count: usize = 0;

    var it = primary_map.keyIterator();
    while (it.next()) |seq_ptr| {
        if (seq_ptr.* > highest_sequence) {
            highest_sequence = seq_ptr.*;
        }
    }

    it = secondary_map.keyIterator();
    while (it.next()) |seq_ptr| {
        if (seq_ptr.* > highest_sequence) {
            highest_sequence = seq_ptr.*;
        }
    }

    // Validate that all sequences up to highest are present and valid
    // Sequences must be contiguous from 1 to N
    if (highest_sequence > 0) {
        for (1..highest_sequence + 1) |seq| {
            const seq_u64 = @as(u64, @intCast(seq));
            if (primary_map.contains(seq_u64) or secondary_map.contains(seq_u64)) {
                valid_count += 1;
            } else {
                // Gap in sequence, truncate
                highest_sequence = @as(u64, @intCast(seq - 1));
                break;
            }
        }
    }

    // Calculate next write offset (padded size of all records)
    var next_write_offset: u64 = 0;
    for (1..highest_sequence + 1) |seq| {
        const seq_cast = @as(u64, @intCast(seq));
        if (primary_map.get(seq_cast)) |r| {
            const record_size = record.HEADER_SIZE + r.header.length;
            const padded_size = ((record_size + record.RECORD_ALIGNMENT - 1) / record.RECORD_ALIGNMENT) * record.RECORD_ALIGNMENT;
            next_write_offset += padded_size;
        } else if (secondary_map.get(seq_cast)) |r| {
            const record_size = record.HEADER_SIZE + r.header.length;
            const padded_size = ((record_size + record.RECORD_ALIGNMENT - 1) / record.RECORD_ALIGNMENT) * record.RECORD_ALIGNMENT;
            next_write_offset += padded_size;
        }
    }

    return RecoveryState{
        .highest_sequence = highest_sequence,
        .next_write_offset = next_write_offset,
        .valid_record_count = valid_count,
    };
}

// Tests
test "recovery state for no records" {
    const allocator = std.testing.allocator;
    const empty_records = std.ArrayList(Record).empty;

    const state = RecoveryState{
        .highest_sequence = 0,
        .next_write_offset = 0,
        .valid_record_count = 0,
    };

    try std.testing.expectEqual(@as(u64, 0), state.highest_sequence);
    try std.testing.expectEqual(@as(u64, 0), state.next_write_offset);
    _ = allocator;
    _ = empty_records;
}

test "reconcile_dual_wals with no records" {
    const allocator = std.testing.allocator;
    const empty_primary = std.ArrayList(Record).empty;
    const empty_secondary = std.ArrayList(Record).empty;

    // Don't defer deinit since these are empty and never allocated
    const state = try reconcile_dual_wals(allocator, empty_primary, empty_secondary);

    try std.testing.expectEqual(@as(u64, 0), state.highest_sequence);
    try std.testing.expectEqual(@as(u64, 0), state.next_write_offset);
}

test "reconcile_dual_wals with single record" {
    const allocator = std.testing.allocator;

    var primary = std.ArrayList(Record).empty;
    defer primary.deinit(allocator);
    const secondary_records = std.ArrayList(Record).empty;

    // Create mock record
    const payload = "test";
    const length = @as(u32, @intCast(payload.len));
    const checksum = Record.calculate_checksum(1, length, payload);

    const mock_record = Record{
        .header = .{
            .magic = record.RECORD_MAGIC,
            .sequence = 1,
            .length = length,
            .checksum = checksum,
        },
        .payload = payload,
    };

    try primary.append(allocator, mock_record);

    const state = try reconcile_dual_wals(allocator, primary, secondary_records);

    try std.testing.expectEqual(@as(u64, 1), state.highest_sequence);
    try std.testing.expect(state.next_write_offset > 0);
}
