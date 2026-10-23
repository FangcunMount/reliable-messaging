SELECT b.store_id,b.writer_epoch,b.schema_version,
 DATE_FORMAT(UTC_TIMESTAMP(6),'%Y-%m-%d %H:%i:%s.%f'),
 e.execution_key,e.state,e.prepare_started,
 DATE_FORMAT(e.lease_until,'%Y-%m-%d %H:%i:%s.%f'),
 w.branch_key,w.state,w.half_outcome,w.final_outcome,
 DATE_FORMAT(w.half_recorded_at,'%Y-%m-%d %H:%i:%s.%f'),
 DATE_FORMAT(w.final_recorded_at,'%Y-%m-%d %H:%i:%s.%f'),
 (w.branch_key IS NULL OR w.branch_key=(
  SELECT MIN(first_w.branch_key) FROM rm_tx_witness first_w
  WHERE first_w.execution_key=e.execution_key)) AS first_branch
FROM rm_tx_binding b
LEFT JOIN rm_tx_execution e ON e.owner=b.owner
LEFT JOIN rm_tx_witness w ON w.execution_key=e.execution_key
WHERE b.owner=?
