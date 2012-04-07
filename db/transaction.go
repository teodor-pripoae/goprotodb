package db

/*
 #cgo LDFLAGS: -ldb
 #include <db.h>
 static inline int db_env_txn_begin(DB_ENV *env, DB_TXN *parent, DB_TXN **txn, u_int32_t flags) {
 	return env->txn_begin(env, parent, txn, flags);
 }
 static inline int db_txn_abort(DB_TXN *txn) {
 	return txn->abort(txn);
 }
 static inline int db_txn_commit(DB_TXN *txn, u_int32_t flags) {
 	return txn->commit(txn, flags);
 }
*/
import "C"

// Transaction isolation level.
type IsolationLevel int

// Available transaction isolation levels.
const (
	ReadCommitted   = IsolationLevel(C.DB_READ_COMMITTED)
	ReadUncommitted = IsolationLevel(C.DB_READ_UNCOMMITTED)
	Snapshot        = IsolationLevel(C.DB_TXN_SNAPSHOT)
)

// Transaction configuration.
type TransactionConfig struct {
	Parent      Transaction // Parent transaction.
	Bulk        bool        // Optimize for bulk insertions.
	NoWait      bool        // Fail instead of waiting for locks.
	NoSync      bool        // Do not flush to log when committing.
	WriteNoSync bool        // Do not flush log when committing.
}

// Transaction in a database environment.
type Transaction struct {
	ptr *C.DB_TXN
}

// Special constant indicating no transaction should be used.
var NoTransaction = Transaction{ptr: nil}

// Perform an operation within a transaction. The transaction is
// automatically committed if the action doesn't return an error. If
// an error occurs, the transaction is automatically aborted. Any
// error is passed through to the caller.
func (env Environment) WithTransaction(isolation IsolationLevel, config *TransactionConfig, action func(Transaction) error) (err error) {
	var parent *C.DB_TXN = NoTransaction.ptr
	var flags C.u_int32_t = C.u_int32_t(isolation)
	if config != nil {
		parent = config.Parent.ptr
		if config.Bulk {
			flags |= C.DB_TXN_BULK
		}
		if config.NoWait {
			flags |= C.DB_TXN_NOWAIT
		}
		if config.NoSync {
			flags |= C.DB_TXN_NOSYNC
		}
		if config.WriteNoSync {
			flags |= C.DB_TXN_WRITE_NOSYNC
		}
	}

	var txn Transaction
	err = check(C.db_env_txn_begin(env.ptr, parent, &txn.ptr, flags))
	if err != nil {
		return
	}

	err = action(txn)
	if err == nil {
		err = check(C.db_txn_commit(txn.ptr, 0))
	} else {
		C.db_txn_abort(txn.ptr)
	}

	return
}
