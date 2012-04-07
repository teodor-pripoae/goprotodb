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
	ErrAgain               = Errno(C.EAGAIN)
	ErrInvalid             = Errno(C.EINVAL)
	ErrNoEntry             = Errno(C.ENOENT)
	ErrExists              = Errno(C.EEXIST)
	ErrAccess              = Errno(C.EACCES)
	ErrNoSpace             = Errno(C.ENOSPC)
	ErrPermission          = Errno(C.EPERM)
	ErrRunRecovery         = Errno(C.DB_RUNRECOVERY)
	ErrVersionMismatch     = Errno(C.DB_VERSION_MISMATCH)
	ErrOldVersion          = Errno(C.DB_OLD_VERSION)
	ErrLockDeadlock        = Errno(C.DB_LOCK_DEADLOCK)
	ErrLockNotGranted      = Errno(C.DB_LOCK_NOTGRANTED)
	ErrReplicaHandleDead   = Errno(C.DB_REP_HANDLE_DEAD)
	ErrReplicaLeaseExpired = Errno(C.DB_REP_LEASE_EXPIRED)
	ErrReplicaLockout      = Errno(C.DB_REP_LOCKOUT)
	ErrBufferTooSmall      = Errno(C.DB_BUFFER_SMALL)
	ErrSecondaryBad        = Errno(C.DB_SECONDARY_BAD)
	ErrForeignConflict     = Errno(C.DB_FOREIGN_CONFLICT)
	ErrKeyExists           = Errno(C.DB_KEYEXIST)
	ErrNotFound            = Errno(C.DB_NOTFOUND)
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
