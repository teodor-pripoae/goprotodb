/* -*- mode: Go; coding: utf-8; -*-
 * This file is part of goprotodb.
 * Copyright (C) 2012 Thomas Chust <chust@web.de>.  All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the Software), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished
 * to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED ASIS, WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package db

import (
	"os"
	"unsafe"
)

/*
 #cgo LDFLAGS: -ldb
 #include <stdlib.h>
 #include <db.h>
 static inline int db_env_set_encrypt(DB_ENV *env, const char *passwd, u_int32_t flags) {
 	return env->set_encrypt(env, passwd, flags);
 }
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
	Password      string      // Encryption password or an empty string.
	Mode          os.FileMode // File creation mode for the environment.
	Create        bool        // Create the environment, if necessary.
	Recover       bool        // Run recovery on the environment, if necessary.
	Transactional bool        // Enable transactions in the environment.
	NoSync        bool        // Do not flush to log when committing.
	WriteNoSync   bool        // Do not flush log when committing.
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

	var flags C.u_int32_t = C.DB_THREAD
	var mode C.int = 0
	if config != nil {
		if len(config.Password) > 0 {
			cpassword := C.CString(config.Password)
			err = check(C.db_env_set_encrypt(env.ptr, cpassword, 0))
			C.free(unsafe.Pointer(cpassword))
			if err != nil {
				return
			}
		}
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

	chome := C.CString(home)
	err = check(C.db_env_open(env.ptr, chome, flags, mode))
	C.free(unsafe.Pointer(chome))

	return
}

// Close the environment.
func (env Environment) Close() (err error) {
	err = check(C.db_env_close(env.ptr, C.u_int32_t(C.DB_FORCESYNC)))
	return
}
