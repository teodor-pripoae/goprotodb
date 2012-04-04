package db

/*
 #cgo LDFLAGS: -ldb
 #include <errno.h>
 #include <db.h>
 */
import "C"

// Status code implementing the error interface.
type Errno int

// Status codes representing common errors.
const (
	ErrAgain = Errno(C.EAGAIN)
	ErrInvalid = Errno(C.EINVAL)
	ErrNoEntry = Errno(C.ENOENT)
	ErrExists = Errno(C.EEXIST)
	ErrAccess = Errno(C.EACCES)
	ErrNoSpace = Errno(C.ENOSPC)
	ErrPermission = Errno(C.EPERM)
	ErrRunRecovery = Errno(C.DB_RUNRECOVERY)
	ErrVersionMismatch = Errno(C.DB_VERSION_MISMATCH)
	ErrOldVersion = Errno(C.DB_OLD_VERSION)
	ErrLockDeadlock = Errno(C.DB_LOCK_DEADLOCK)
	ErrLockNotGranted = Errno(C.DB_LOCK_NOTGRANTED)
	ErrReplicaHandleDead = Errno(C.DB_REP_HANDLE_DEAD)
	ErrReplicaLeaseExpired = Errno(C.DB_REP_LEASE_EXPIRED)
	ErrReplicaLockout = Errno(C.DB_REP_LOCKOUT)
	ErrBufferTooSmall = Errno(C.DB_BUFFER_SMALL)
	ErrSecondaryBad = Errno(C.DB_SECONDARY_BAD)
	ErrForeignConflict = Errno(C.DB_FOREIGN_CONFLICT)
	ErrKeyExists = Errno(C.DB_KEYEXIST)
	ErrNotFound = Errno(C.DB_NOTFOUND)
)

// Turn a status code into a human readable message.
func (err Errno) Error() string {
	return C.GoString(C.db_strerror(C.int(err)))
}

// Check a function result and return an error if necessary.
func check(rc C.int) (err error) {
	if rc != 0 {
		err = Errno(rc)
	}
	return
}
