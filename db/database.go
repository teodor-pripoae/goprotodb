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
	"code.google.com/p/goprotobuf/proto"
	"os"
	"unsafe"
)

/*
 #cgo LDFLAGS: -ldb
 #include <stdlib.h>
 #include <db.h>
 static inline int db_set_encrypt(DB *db, const char *passwd, u_int32_t flags) {
 	return db->set_encrypt(db, passwd, flags);
 }
 static inline int db_open(DB *db, DB_TXN *txn, const char *file, const char *database, DBTYPE type, u_int32_t flags, int mode) {
 	return db->open(db, txn, file, database, type, flags, mode);
 }
 static inline int db_close(DB *db, u_int32_t flags) {
 	return db->close(db, flags);
 }
 static inline int db_get_type(DB *db, DBTYPE *type) {
 	return db->get_type(db, type);
 }
 static inline int db_put(DB *db, DB_TXN *txn, DBT *key, DBT *data, u_int32_t flags) {
 	return db->put(db, txn, key, data, flags);
 }
 static inline int db_get(DB *db, DB_TXN *txn, DBT *key, DBT *data, u_int32_t flags) {
 	return db->get(db, txn, key, data, flags);
 }
 static inline int db_del(DB *db, DB_TXN *txn, DBT *key, u_int32_t flags) {
 	return db->del(db, txn, key, flags);
 }
 static inline int db_cursor(DB *db, DB_TXN *txn, DBC **cursor, u_int32_t flags) {
 	return db->cursor(db, txn, cursor, flags);
 }
 static inline int db_cursor_close(DBC *cur) {
 	return cur->close(cur);
 }
 static inline int db_cursor_get(DBC *cur, DBT *key, DBT *data, u_int32_t flags) {
 	return cur->get(cur, key, data, flags);
 }
 static inline int db_cursor_del(DBC *cur, u_int32_t flags) {
 	return cur->del(cur, flags);
 }
*/
import "C"

// Type of databases.
type DatabaseType int

// Available database types.
const (
	BTree    = DatabaseType(C.DB_BTREE)
	Hash     = DatabaseType(C.DB_HASH)
	Numbered = DatabaseType(C.DB_RECNO)
	Queue    = DatabaseType(C.DB_QUEUE)
	Unknown  = DatabaseType(C.DB_UNKNOWN)
)

// Database configuration.
type DatabaseConfig struct {
	Create          bool         // Create the database, if necessary.
	Mode            os.FileMode  // File creation mode for the database.
	Password        string       // Encryption password or an empty string.
	Name            string       // Identifier of the database inside the file.
	Type            DatabaseType // Type of database to create
	ReadUncommitted bool         // Enable support for read-uncommitted isolation.
	Snapshot        bool         // Enable support for snapshot isolation.
}

// Database.
type Database struct {
	ptr *C.DB
}

// Open a database in the given file and environment.
func OpenDatabase(env Environment, txn Transaction, file string, config *DatabaseConfig) (db Database, err error) {
	err = check(C.db_create(&db.ptr, env.ptr, 0))
	if err == nil {
		defer func() {
			if err != nil && db.ptr != nil {
				C.db_close(db.ptr, 0)
				db.ptr = nil
			}
		}()
	} else {
		return
	}

	var mode C.int = 0
	var flags C.u_int32_t = C.DB_THREAD
	var cfile, cpassword, cname *C.char
	var dbtype C.DBTYPE = C.DB_UNKNOWN

	if len(file) > 0 {
		cfile = C.CString(file)
		defer C.free(unsafe.Pointer(cfile))
	}

	if config != nil {
		if config.Create {
			flags |= C.DB_CREATE
		}
		if config.Mode != 0 {
			mode = C.int(config.Mode)
		}
		if len(config.Password) > 0 {
			cpassword := C.CString(config.Password)
			defer C.free(unsafe.Pointer(cpassword))
		}
		if len(config.Name) > 0 {
			cname = C.CString(config.Name)
			defer C.free(unsafe.Pointer(cname))
		}
		if config.Type != 0 {
			dbtype = C.DBTYPE(config.Type)
		}
		if config.ReadUncommitted {
			flags |= C.DB_READ_UNCOMMITTED
		}
		if config.Snapshot {
			flags |= C.DB_MULTIVERSION
		}
	}

	if cpassword != nil {
		err = check(C.db_set_encrypt(db.ptr, cpassword, 0))
		if err != nil {
			return
		}
	}

	err = check(C.db_open(db.ptr, txn.ptr, cfile, cname, dbtype, flags, mode))

	return
}

// Close the database.
func (db Database) Close() (err error) {
	err = check(C.db_close(db.ptr, 0))
	return
}

// Get the type of the database.
func (db Database) DatabaseType() (dbtype DatabaseType, err error) {
	var cdbtype C.DBTYPE
	err = check(C.db_get_type(db.ptr, &cdbtype))
	dbtype = DatabaseType(cdbtype)
	return
}

// Interface for storable records.
type Record interface {
	// Obtain a pointer to the record key. If the record currently
	// has no key, it must be allocated. The result must be
	// serializable using protobuf or, for storage in queue and
	// records databases, a *uint32.
	RecordKey() interface{}

	// Obtain a pointer to a copy of the record without its
	// key. The result must be serializable using protobuf.
	RecordWithoutKey() interface{}
}

// Marshal a protobuf struct into a database thang.
func marshalDBT(dbt *C.DBT, val interface{}) (err error) {
	buf, err := proto.Marshal(val)
	if err != nil {
		return
	}

	if len(buf) > 0 {
		dbt.data = unsafe.Pointer(&buf[0])
		dbt.size = C.u_int32_t(len(buf))
	} else {
		dbt.data = nil
		dbt.size = 0
	}

	return
}

// Marshal the key of a record into a database thang.
func (db Database) marshalKey(dbt *C.DBT, rec Record) (err error) {
	dbtype, err := db.DatabaseType()
	if err != nil {
		return
	}

	switch dbtype {
	case Numbered, Queue:
		dbt.data = unsafe.Pointer(rec.RecordKey().(*uint32))
		dbt.size = 4

	default:
		err = marshalDBT(dbt, rec.RecordKey())
	}

	return
}

// Marshal the data of a record into a database thang.
func (db Database) marshalData(dbt *C.DBT, rec Record) (err error) {
	err = marshalDBT(dbt, rec.RecordWithoutKey())
	return
}

// Unmarshal a protobuf struct from a database thang.
func unmarshalDBT(dbt *C.DBT, val interface{}) (err error) {
	buf := C.GoBytes(dbt.data, C.int(dbt.size))
	err = proto.Unmarshal(buf, val)
	return
}

// Unmarshal the key of a record from a database thang.
func (db Database) unmarshalKey(dbt *C.DBT, rec Record) (err error) {
	dbtype, err := db.DatabaseType()
	if err != nil {
		return
	}

	switch dbtype {
	case Numbered, Queue:
		if dbt.size == 4 {
			*rec.RecordKey().(*uint32) = *(*uint32)(dbt.data)
		} else {
			panic("key size does not match record number data type")
		}

	default:
		err = unmarshalDBT(dbt, rec.RecordKey())
	}

	return
}

// Unmarshal the data of a record from a database thang.
func (db Database) unmarshalData(dbt *C.DBT, rec Record) (err error) {
	err = unmarshalDBT(dbt, rec)
	return
}

// Store records in the database. In combination with a queue or
// records database the append flags causes the keys of the records to
// be set to fresh record numbers, for any other database it prevents
// an existing record with the same key from being overwritten.
func (db Database) Put(txn Transaction, append bool, recs ...Record) (err error) {
	dbtype, err := db.DatabaseType()
	if err != nil {
		return
	}

	var key, data C.DBT
	var flags C.u_int32_t = 0

	if append {
		key.flags |= C.DB_DBT_USERMEM

		switch dbtype {
		case Numbered, Queue:
			flags |= C.DB_APPEND
		default:
			flags |= C.DB_NOOVERWRITE
		}
	} else {
		key.flags |= C.DB_DBT_READONLY
	}

	data.flags |= C.DB_DBT_READONLY

	for _, rec := range recs {
		err = db.marshalKey(&key, rec)
		if err == nil {
			key.ulen = key.size
		} else {
			return
		}

		err = db.marshalData(&data, rec)
		if err != nil {
			return
		}

		err = check(C.db_put(db.ptr, txn.ptr, &key, &data, flags))
		if err != nil {
			return
		}
	}

	return
}

// Get records from the database. The consume flag makes sense only in
// combination with a queue database and causes the operation to wait
// for and obtain the next enqueued record.
func (db Database) Get(txn Transaction, consume bool, recs ...Record) (err error) {
	var key, data C.DBT
	var flags C.u_int32_t = 0

	if consume {
		key.flags |= C.DB_DBT_USERMEM
		flags |= C.DB_CONSUME_WAIT
	} else {
		key.flags |= C.DB_DBT_READONLY
	}

	data.flags |= C.DB_DBT_REALLOC
	defer C.free(data.data)

	for _, rec := range recs {
		err = db.marshalKey(&key, rec)
		if err == nil {
			key.ulen = key.size
		} else {
			return
		}

		err = check(C.db_get(db.ptr, txn.ptr, &key, &data, flags))
		if err != nil {
			return
		}

		err = db.unmarshalData(&data, rec)
		if err != nil {
			return
		}
	}

	return
}

// Delete records from the database.
func (db Database) Del(txn Transaction, recs ...Record) (err error) {
	var key C.DBT

	key.flags |= C.DB_DBT_READONLY

	for _, rec := range recs {
		err = db.marshalKey(&key, rec)
		if err != nil {
			return
		}

		err = check(C.db_del(db.ptr, txn.ptr, &key, 0))
		if err != nil {
			return
		}
	}

	return
}

// Database cursor.
type Cursor struct {
	Database
	ptr *C.DBC
}

// Obtain a cursor over the database.
func (db Database) Cursor(txn Transaction) (cur Cursor, err error) {
	cur.Database = db
	err = check(C.db_cursor(db.ptr, txn.ptr, &cur.ptr, 0))
	return
}

// Close the cursor.
func (cur Cursor) Close() (err error) {
	err = check(C.db_cursor_close(cur.ptr))
	return
}

// Retrieve the first record with matching key from the database. If
// exact is false, the first record with a key greater than or equal
// to the given one is fetched; this operation mode only makes sense
// in combination with a BTree database.
func (cur Cursor) Set(exact bool, rec Record) (err error) {
	var key, data C.DBT

	if exact {
		key.flags |= C.DB_DBT_READONLY
	} else {
		key.flags |= C.DB_DBT_USERMEM
	}

	data.flags |= C.DB_DBT_REALLOC
	defer C.free(data.data)

	err = cur.marshalKey(&key, rec)
	if err == nil {
		key.ulen = key.size
	} else {
		return
	}

	err = check(C.db_cursor_get(cur.ptr, &key, &data, C.DB_SET))
	if err != nil {
		return
	}

	if !exact {
		err = cur.unmarshalKey(&key, rec)
		if err != nil {
			return
		}
	}

	err = cur.unmarshalData(&data, rec)

	return
}

// Retrieve the first record of the database.
func (cur Cursor) First(rec Record) (err error) {
	var key, data C.DBT

	key.flags |= C.DB_DBT_REALLOC
	defer C.free(key.data)
	data.flags |= C.DB_DBT_REALLOC
	defer C.free(data.data)

	err = check(C.db_cursor_get(cur.ptr, &key, &data, C.DB_FIRST))
	if err != nil {
		return
	}

	err = cur.unmarshalKey(&key, rec)
	if err != nil {
		return
	}

	err = cur.unmarshalData(&data, rec)

	return
}

// Retrieve the next record from the cursor.
func (cur Cursor) Next(rec Record) (err error) {
	var key, data C.DBT

	key.flags |= C.DB_DBT_REALLOC
	defer C.free(key.data)
	data.flags |= C.DB_DBT_REALLOC
	defer C.free(data.data)

	err = check(C.db_cursor_get(cur.ptr, &key, &data, C.DB_NEXT))
	if err != nil {
		return
	}

	err = cur.unmarshalKey(&key, rec)
	if err != nil {
		return
	}

	err = cur.unmarshalData(&data, rec)

	return
}

// Retrieve the last record of the database.
func (cur Cursor) Last(rec Record) (err error) {
	var key, data C.DBT

	key.flags |= C.DB_DBT_REALLOC
	defer C.free(key.data)
	data.flags |= C.DB_DBT_REALLOC
	defer C.free(data.data)

	err = check(C.db_cursor_get(cur.ptr, &key, &data, C.DB_LAST))
	if err != nil {
		return
	}

	err = cur.unmarshalKey(&key, rec)
	if err != nil {
		return
	}

	err = cur.unmarshalData(&data, rec)

	return
}

// Retrieve the previous record from the cursor.
func (cur Cursor) Prev(rec Record) (err error) {
	var key, data C.DBT

	key.flags |= C.DB_DBT_REALLOC
	defer C.free(key.data)
	data.flags |= C.DB_DBT_REALLOC
	defer C.free(data.data)

	err = check(C.db_cursor_get(cur.ptr, &key, &data, C.DB_PREV))
	if err != nil {
		return
	}

	err = cur.unmarshalKey(&key, rec)
	if err != nil {
		return
	}

	err = cur.unmarshalData(&data, rec)

	return
}

// Delete the current record at the cursor.
func (cur Cursor) Del() (err error) {
	err = check(C.db_cursor_del(cur.ptr, 0))
	return
}
