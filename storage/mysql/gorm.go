package mysql

import (
	"database/sql"
	"errors"
	"gorm.io/gorm"
)

// BindGORM borrows the original SQL transaction exposed by GORM 1.30.0.
// Ordinary DB handles and unknown wrappers fail closed; no Begin or Commit is
// performed. The host still owns rollback on Append failure. A transaction that
// was already completed will fail at Append through database/sql.
func BindGORM(tx *gorm.DB) (*Appender, error) {
	if tx == nil || tx.Error != nil || tx.Statement == nil {
		return nil, errors.New("active GORM transaction required")
	}
	pool := tx.Statement.ConnPool
	if prepared, ok := pool.(*gorm.PreparedStmtTX); ok {
		pool = prepared.Tx
	}
	raw, ok := pool.(*sql.Tx)
	if !ok || raw == nil {
		return nil, errors.New("GORM connection is not a supported original SQL transaction")
	}
	return Bind(raw)
}
