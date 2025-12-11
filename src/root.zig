//! Write-ahead log library with io_uring, checksums, and LSE protection.

pub const record = @import("record.zig");
pub const wal = @import("wal.zig");
pub const io_manager = @import("io_manager.zig");
pub const verification = @import("verification.zig");
pub const recovery = @import("recovery.zig");

// Re-export key types for convenience
pub const Record = record.Record;
pub const RecordHeader = record.RecordHeader;

pub const WAL = wal.WAL;
pub const PendingOp = wal.PendingOp;
pub const ReplayCallback = wal.ReplayCallback;

pub const UserDataTag = io_manager.UserDataTag;

pub const VerifyResult = verification.VerifyResult;
pub const FileId = verification.FileId;

pub const RecoveryState = recovery.RecoveryState;
