package db

import (
	"os"
	"unsafe"
)

/*
 #cgo LDFLAGS: -ldb
 #include <stdlib.h>
 #include <db.h>
 static inline int db_env_open(DB_ENV *env, const char *home, u_int32_t flags, int mode) {
 	return env->open(env, home, flags, mode);
 }
 static inline int db_env_close(DB_ENV *env, u_int32_t flags) {
 	return env->close(env, flags);
 }
 */
import "C"

// Database environment configuration.
type EnvironmentConfig struct {
	Mode os.FileMode    // File creation mode for the environment.
	Create bool         // Create the environment, if necessary.
	Recover bool        // Run recovery on the environment, if necessary.
	Transactional bool  // Enable transactions in the environment.
	NoSync bool         // Do not flush to log when committing.
	WriteNoSync bool    // Do not flush log when committing.
}

// Database environment.
type Environment struct {
	ptr *C.DB_ENV
}

// Special constant to indicate no environment should be used.
var NoEnvironment = Environment{ptr: nil}

// Open an environment at the given home path.
func OpenEnvironment(home string, config *EnvironmentConfig) (env Environment, err error) {
	err = check(C.db_env_create(&env.ptr, 0))
	if err != nil {
		return
	}

	var chome *C.char = C.CString(home)
	var flags C.u_int32_t = C.DB_THREAD
	var mode C.int = 0755
	if config != nil {
		mode = C.int(config.Mode)
		if config.Create {
			flags |= C.DB_CREATE
		}
		if config.Recover {
			flags |= C.DB_REGISTER | C.DB_FAILCHK | C.DB_RECOVER
		}
		if config.Transactional {
			flags |= C.DB_INIT_TXN | C.DB_INIT_MPOOL
		}
		if config.NoSync {
			flags |= C.DB_TXN_NOSYNC
		}
		if config.WriteNoSync {
			flags |= C.DB_TXN_WRITE_NOSYNC
		}
	}

	err = check(C.db_env_open(env.ptr, chome, flags, mode))

	C.free(unsafe.Pointer(chome))

	return
}

// Close the environment.
func (env Environment) Close() (err error) {
	err = check(C.db_env_close(env.ptr, C.u_int32_t(C.DB_FORCESYNC)))
	return
}
