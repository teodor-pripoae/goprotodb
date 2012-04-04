package db

/*
 #include <db.h>
 */
import "C"

// Flag constants for database operations.
const (
	ConcurrentDataStore = uint32(C.DB_INIT_CDB)
	Locking = uint32(C.DB_INIT_LOCK)
	Logging = uint32(C.DB_INIT_LOG)
	MemoryPool = uint32(C.DB_INIT_MPOOL)
	Replication = uint32(C.DB_INIT_REP)
	Transactions = uint32(C.DB_INIT_TXN)
	Register = uint32(C.DB_REGISTER)
	Recover = uint32(C.DB_RECOVER)
	RecoverFatal = uint32(C.DB_RECOVER_FATAL)
	UseEnvironment = uint32(C.DB_USE_ENVIRON)
	UseEnvironmentRoot = uint32(C.DB_USE_ENVIRON_ROOT)
	Create = uint32(C.DB_CREATE)
	Lockdown = uint32(C.DB_LOCKDOWN)
	PrivateMemory = uint32(C.DB_PRIVATE)
	SharedMemomy = uint32(C.DB_SYSTEM_MEM)
	FreeThreaded = uint32(C.DB_THREAD)
	ReadCommitted = uint32(C.DB_READ_COMMITTED)
	ReadUncommitted = uint32(C.DB_READ_UNCOMMITTED)
	NoSync = uint32(C.DB_TXN_NOSYNC)
	NoWait = uint32(C.DB_TXN_NOWAIT)
	Snapshot = uint32(C.DB_TXN_SNAPSHOT)
	Sync = uint32(C.DB_TXN_SYNC)
	Wait = uint32(C.DB_TXN_WAIT)
	WriteNoSync = uint32(C.DB_TXN_WRITE_NOSYNC)
	AutoCommit = uint32(C.DB_AUTO_COMMIT)
	Exclusive = uint32(C.DB_EXCL)
	MultiVersion = uint32(C.DB_MULTIVERSION)
	NoMMap = uint32(C.DB_NOMMAP)
	ReadOnly = uint32(C.DB_RDONLY)
	Truncate = uint32(C.DB_TRUNCATE)
	Consume = uint32(C.DB_CONSUME)
	ConsumeWait = uint32(C.DB_CONSUME_WAIT)
	IgnoreLease = uint32(C.DB_IGNORE_LEASE)
	ReadModifyWrite = uint32(C.DB_RMW)
	Append = uint32(C.DB_APPEND)
	NoDuplicates = uint32(C.DB_NODUPDATA)
	NoOverwrite = uint32(C.DB_NOOVERWRITE)
	OverwriteDuplicates = uint32(C.DB_OVERWRITE_DUP)
)
