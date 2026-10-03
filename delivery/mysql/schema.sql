-- Reference only. The host names/migrates/owns this table in its business database.
-- Separate from the existing Go broker-confirmed outbox; no automatic migration.
CREATE TABLE rm_durable_outbox (
  producer VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  destination VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  message_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
  body_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  body MEDIUMBLOB NOT NULL,
  wire MEDIUMBLOB NOT NULL,
  topic VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  aggregate_key VARCHAR(192) COLLATE utf8mb4_bin NOT NULL,
  aggregate_sequence BIGINT UNSIGNED NOT NULL,
  ordered BOOLEAN NOT NULL,
  requires_receipt BOOLEAN NOT NULL,
  stage VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  attempts BIGINT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME(6) NOT NULL,
  created_at DATETIME(6) NOT NULL,
  published_at DATETIME(6) NULL,
  confirmed_at DATETIME(6) NULL,
  error_code VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
  PRIMARY KEY (producer, destination, message_id),
  INDEX pending_scan (stage, available_at, message_id),
  INDEX aggregate_order (producer, destination, aggregate_key, ordered, aggregate_sequence, stage)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
