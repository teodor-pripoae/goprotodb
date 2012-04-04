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

// Database environment.
type Environment struct {
	ptr *C.DB_ENV
}

// Special constant to indicate no environment should be used.
var NoEnvironment = Environment{ptr: nil}

// Open an environment at the given home path.
func OpenEnvironment(home string, flags uint32, mode os.FileMode) (env Environment, err error) {
	err = check(C.db_env_create(&env.ptr, 0))
	if err != nil {
		return
	}

	chome := C.CString(home)
	err = check(C.db_env_open(env.ptr, chome, C.u_int32_t(flags), C.int(mode)))
	C.free(unsafe.Pointer(chome))

	return
}

// Close the environment.
func (env Environment) Close(flags uint32) (err error) {
	err = check(C.db_env_close(env.ptr, C.u_int32_t(flags)))
	return
}
