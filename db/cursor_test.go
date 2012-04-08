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
	"testing"
)

func TestCursor(t *testing.T) {
	withDb(t, BTree, func(db Database) {
		rec0 := &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("car")},
			Val: proto.String("foo"),
		}
		rec1 := &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("caddr")},
			Val: proto.String("bar"),
		}
		rec2 := &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("cdaddar")},
			Val: proto.String("baz"),
		}

		err := db.Put(NoTransaction, false, rec0, rec1, rec2)
		if err != nil {
			t.Error("Put failed:", err)
		}

		cur, err := db.Cursor(NoTransaction)
		if err != nil {
			t.Fatal("Failed to create cursor:", err)
		}

		rec := &TestRecord{
			Key: &TestRecord_Key{Val: proto.String("cadr")},
		}

		err = cur.Set(true, rec)
		if err == nil {
			t.Error("Illegal cursor set succeeded:", rec)
		}

		err = cur.Set(false, rec)
		if err != nil {
			t.Error("Cursor set failed:", err)
		}
		if *rec1.Key.Val != *rec.Key.Val {
			t.Error("Retrieved key mismatch:", rec1, rec)
		}
		if *rec1.Val != *rec.Val {
			t.Error("Retrieved value mismatch:", rec1, rec)
		}

		err = cur.Next(rec)
		if err != nil {
			t.Error("Cursor walk failed:", err)
		}
		if *rec2.Key.Val != *rec.Key.Val {
			t.Error("Retrieved key mismatch:", rec2, rec)
		}
		if *rec2.Val != *rec.Val {
			t.Error("Retrieved value mismatch:", rec2, rec)
		}

		err = cur.Next(rec)
		if err == nil {
			t.Error("Illegal cursor walk succeeded:", rec)
		}

		err = cur.Prev(rec)
		if err != nil {
			t.Error("Cursor walk failed:", err)
		}
		if *rec1.Key.Val != *rec.Key.Val {
			t.Error("Retrieved key mismatch:", rec1, rec)
		}
		if *rec1.Val != *rec.Val {
			t.Error("Retrieved value mismatch:", rec1, rec)
		}

		err = cur.Del()
		if err != nil {
			t.Error("Cursor delete failed:", err)
		}

		err = cur.Close()
		if err != nil {
			t.Error("Cursor close failed:", err)
		}
	})
}
