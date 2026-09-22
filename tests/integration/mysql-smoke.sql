CREATE DATABASE rm_contract_test;
USE rm_contract_test;
CREATE TABLE business (id VARCHAR(64) PRIMARY KEY) ENGINE=InnoDB;
CREATE TABLE outbox (id VARCHAR(64) PRIMARY KEY, payload TEXT NOT NULL) ENGINE=InnoDB;
START TRANSACTION;
INSERT INTO business VALUES ('committed');
INSERT INTO outbox VALUES ('committed', 'unchanged');
COMMIT;
START TRANSACTION;
INSERT INTO business VALUES ('rolled-back');
INSERT INTO outbox VALUES ('rolled-back', 'unchanged');
ROLLBACK;
SELECT IF(
  (SELECT COUNT(*) FROM business WHERE id='committed')=1 AND
  (SELECT COUNT(*) FROM outbox WHERE id='committed')=1 AND
  (SELECT COUNT(*) FROM business WHERE id='rolled-back')=0 AND
  (SELECT COUNT(*) FROM outbox WHERE id='rolled-back')=0,
  'PASS MySQL commit/rollback', 'FAIL MySQL transaction');
