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

package protodb

import (
	"code.google.com/p/goprotobuf/proto"
	"testing"
)

func TestPutGet(t *testing.T) {
	withDb(t, BTree, func(db Database) {
		rec0 := &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("hello")},
			Val: proto.String("world"),
		}

		err := db.Put(NoTransaction, false, rec0)
		if err != nil {
			t.Error("Put failed:", err)
		}

		rec1 := &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("hello")},
		}

		err = db.Get(NoTransaction, false, rec1)
		if err != nil {
			t.Error("Get failed:", err)
		}
		if *rec0.Key.Val != *rec1.Key.Val {
			t.Error("Retrieved key mismatch:", rec0, rec1)
		}
		if *rec0.Val != *rec1.Val {
			t.Error("Retrieved value mismatch:", rec0, rec1)
		}

		rec1 = &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("foobar")},
		}

		err = db.Get(NoTransaction, false, rec1)
		if err != ErrNotFound {
			t.Error("Illegal get succeeded:", rec1, err)
		}

		return
	})
}

func TestPutGetNumbered(t *testing.T) {
	withDb(t, Numbered, func(db Database) {
		rec0 := &NumberedTestRecord{
			Val: proto.String("world"),
		}

		err := db.Put(NoTransaction, true, rec0)
		if err != nil {
			t.Error("Put failed:", err)
		}

		rec1 := &NumberedTestRecord{
			Key: rec0.Key,
		}

		err = db.Get(NoTransaction, false, rec1)
		if err != nil {
			t.Error("Get failed:", err)
		}
		if *rec0.Val != *rec1.Val {
			t.Error("Retrieved value mismatch:", rec0, rec1)
		}

		return
	})
}
