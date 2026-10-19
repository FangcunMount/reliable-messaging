-- Explicit host migration template. No constructor executes this DDL.
CREATE TABLE rm_tx_binding (
 owner VARBINARY(128) NOT NULL PRIMARY KEY,
 store_id VARBINARY(128) NOT NULL,
 writer_epoch VARBINARY(128) NOT NULL,
 schema_version INT NOT NULL
) ENGINE=InnoDB;
CREATE TABLE rm_tx_request (
 request_key BINARY(32) NOT NULL PRIMARY KEY,
 owner VARBINARY(128) NOT NULL,
 scope BLOB NOT NULL,
 idempotency_key BLOB NOT NULL,
 input_hash BINARY(32) NOT NULL,
 active_execution BINARY(32) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE rm_tx_execution (
 execution_key BINARY(32) NOT NULL PRIMARY KEY,
 request_key BINARY(32) NOT NULL,
 owner VARBINARY(128) NOT NULL,
 operation_id BLOB NOT NULL,
 writer_epoch VARBINARY(128) NOT NULL,
 fence BIGINT NOT NULL,
 token BINARY(32) NOT NULL,
 plan LONGBLOB NOT NULL,
 state VARBINARY(16) NOT NULL,
 lease_until DATETIME(6) NOT NULL,
 guard_version BIGINT NOT NULL DEFAULT 0,
 prepare_started BOOLEAN NOT NULL DEFAULT FALSE,
 INDEX rm_tx_expiry(owner,state,lease_until)
) ENGINE=InnoDB;
CREATE TABLE rm_tx_witness (
 execution_key BINARY(32) NOT NULL,
 branch_key BINARY(32) NOT NULL,
 reference BLOB NOT NULL,
 state VARBINARY(16) NOT NULL,
 PRIMARY KEY(execution_key,branch_key)
) ENGINE=InnoDB;
