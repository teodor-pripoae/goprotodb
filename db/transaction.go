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
func (env Environment) WithTransaction(parent Transaction, flags uint32, action func(Transaction) error) (err error) {
	var txn Transaction
	err = check(C.db_env_txn_begin(env.ptr, parent.ptr, &txn.ptr, C.u_int32_t(flags)))
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
