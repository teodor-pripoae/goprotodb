package db

import (
	"os"
	"reflect"
	"unsafe"
	"code.google.com/p/goprotobuf/proto"
)

/*
 #cgo LDFLAGS: -ldb
 #include <stdlib.h>
 #include <db.h>
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
 */
import "C"

// Type of databases.
type DatabaseType int

// Available database types.
const (
	BTree = DatabaseType(C.DB_BTREE)
	Hash = DatabaseType(C.DB_HASH)
	Records = DatabaseType(C.DB_RECNO)
	Queue = DatabaseType(C.DB_QUEUE)
	Unknown = DatabaseType(C.DB_UNKNOWN)
)

// Database configuration.
type DatabaseConfig struct {
	Mode os.FileMode      // File creation mode for the environment.
	Create bool           // Create the database, if necessary.
	ReadUncommitted bool  // Enable support for read-uncommitted isolation.
	Snapshot bool         // Enable support for snapshot isolation.
}

// Database.
type Database struct {
	ptr *C.DB
}

// Open a database in the given file and environment.
func OpenDatabase(env Environment, txn Transaction, file, name string, dbtype DatabaseType, config *DatabaseConfig) (db Database, err error) {
	err = check(C.db_create(&db.ptr, env.ptr, 0))
	if err != nil {
		return
	}

	var cfile, cname *C.char
	if len(file) > 0 {
		cfile = C.CString(file)
	}
	if len(name) > 0 {
		cname = C.CString(name)
	}

	var flags C.u_int32_t = C.DB_THREAD
	var mode C.int = 0
	if config != nil {
		mode = C.int(config.Mode)
		if config.Create {
			flags |= C.DB_CREATE
		}
		if config.ReadUncommitted {
			flags |= C.DB_READ_UNCOMMITTED
		}
		if config.Snapshot {
			flags |= C.DB_MULTIVERSION
		}
	}

	err = check(C.db_open(db.ptr, txn.ptr, cfile, cname, C.DBTYPE(dbtype), flags, mode))

	C.free(unsafe.Pointer(cfile))
	C.free(unsafe.Pointer(cname))

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

// Store some values under the given key in the database. For Records
// and Queue databases, the key must be a pointer to a RecordId
// variable; in append mode that variable will be set to the new
// record number.
func (db Database) Insert(txn Transaction, flags uint32, key interface{}, data ...interface{}) (err error) {
	var buf []byte
	var ckey, cdata C.DBT

	if flags & Append == 0 {
		buf, err = proto.Marshal(key)
		if err != nil {
			return
		}

		if ckey.size = C.u_int32_t(len(buf)); ckey.size > 0 {
			ckey.data = unsafe.Pointer(&buf[0])
		}
	}

	for _, dati := range data {
		buf, err = proto.Marshal(dati)
		if err != nil {
			return
		}

		if cdata.size = C.u_int32_t(len(buf)); cdata.size > 0 {
			cdata.data = unsafe.Pointer(&buf[0])
		} else {
			cdata.data = nil
		}

		err = check(C.db_put(db.ptr, txn.ptr, &ckey, &cdata, C.u_int32_t(flags)))
		if err != nil {
			return
		}
	}

	if flags & Append != 0 {
		buf = C.GoBytes(ckey.data, C.int(ckey.size))
		err = proto.Unmarshal(buf, key)
	}

	return
}

// Remove a key and its associated values from the database. For
// Records and Queue databases, the key must be a pointer to a
// RecordId variable.
func (db Database) Remove(txn Transaction, flags uint32, key interface{}) (err error) {
	var buf []byte
	var ckey C.DBT

	buf, err = proto.Marshal(key)
	if err != nil {
		return
	}

	if ckey.size = C.u_int32_t(len(buf)); ckey.size > 0 {
		ckey.data = unsafe.Pointer(&buf[0])
	}

	err = check(C.db_del(db.ptr, txn.ptr, &ckey, C.u_int32_t(flags)))
	return
}

// Query object.
type Query struct {
	db Database
	cur *C.DBC
	key interface{}
}

// Initialize a query for the given key.
func (db Database) Find(txn Transaction, flags uint32, key interface{}) (qry *Query, err error) {
	if key != nil {
		qry = &Query{db: db, key: key}
	} else {
		qry = &Query{db: db}
	}
	err = check(C.db_cursor(db.ptr, txn.ptr, &qry.cur, C.u_int32_t(flags)))
	return
}

// Finish a query.
func (qry *Query) Close() (err error) {
	err = check(C.db_cursor_close(qry.cur))
	return
}

// Extract the next duplicate from the query.
func (qry *Query) One(data interface{}) (err error) {
	var buf []byte
	var ckey, cdata C.DBT
	var flags uint32

	if qry.key != nil {
		buf, err = proto.Marshal(qry.key)
		if err != nil {
			return
		}

		if ckey.size = C.u_int32_t(len(buf)); ckey.size > 0 {
			ckey.data = unsafe.Pointer(&buf[0])
		}

		flags = C.DB_SET
	} else {
		flags = C.DB_NEXT_DUP
	}

	err = check(C.db_cursor_get(qry.cur, &ckey, &cdata, C.u_int32_t(flags)))
	if err == nil {
		qry.key = nil
	} else {
		return
	}

	buf = C.GoBytes(cdata.data, C.int(cdata.size))
	err = proto.Unmarshal(buf, data)

	return
}

// Extract all duplicates from the query.
func (qry *Query) All(data interface{}) (err error) {
	datav := reflect.ValueOf(data)
	if datav.Kind() != reflect.Ptr || datav.Elem().Kind() != reflect.Slice {
		panic("data argument must be a slice address")
	}

	slicev := datav.Elem().Slice(0, 0)
	elemt := slicev.Type().Elem()
	for {
		elemv := reflect.New(elemt)
		err = qry.One(elemv.Interface())
		switch err {
		case nil:
			slicev = reflect.Append(slicev, elemv.Elem())

		case ErrNotFound:
			err = nil
			break

		default:
			return
		}
	}

	datav.Elem().Set(slicev)
	return
}

// Extract the next key and data from the query.
func (qry *Query) Next(key interface{}, data interface{}) (err error) {
	var buf []byte
	var ckey, cdata C.DBT
	var flags uint32

	if qry.key != nil {
		buf, err = proto.Marshal(qry.key)
		if err != nil {
			return
		}

		if ckey.size = C.u_int32_t(len(buf)); ckey.size > 0 {
			ckey.data = unsafe.Pointer(&buf[0])
		}

		flags = C.DB_SET
	} else {
		flags = C.DB_NEXT
	}

	err = check(C.db_cursor_get(qry.cur, &ckey, &cdata, C.u_int32_t(flags)))
	if err == nil {
		qry.key = nil
	} else {
		return
	}

	buf = C.GoBytes(ckey.data, C.int(ckey.size))
	err = proto.Unmarshal(buf, key)
	if err != nil {
		return
	}

	buf = C.GoBytes(cdata.data, C.int(cdata.size))
	err = proto.Unmarshal(buf, data)

	return
}
