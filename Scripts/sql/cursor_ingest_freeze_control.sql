-- Fabric lakehouse / warehouse: run once in AI_Medallion (Gold schema).
-- Inserts are append-only (one row per frozen Month_No + product).

CREATE TABLE IF NOT EXISTS Gold.ingest_freeze_control (
  product     STRING  NOT NULL COMMENT 'cursor | copilot | all',
  Month_No    INT     NOT NULL COMMENT 'YYYYMM e.g. 202609',
  frozen_at   TIMESTAMP NOT NULL,
  frozen_by   STRING,
  notes       STRING
) USING DELTA;

-- Example: freeze September 2026 for Cursor
-- INSERT INTO Gold.ingest_freeze_control VALUES
--   ('cursor', 202609, current_timestamp(), 'your.name', 'Finance sign-off Sep 2026');
