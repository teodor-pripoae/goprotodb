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

func TestPutGetDel(t *testing.T) {
	withEnvDb(t, Numbered, func(env Environment, db Database) {
		rec0 := &NumberedTestRecord{
			Val: proto.String("blubb"),
		}

		err := env.WithTransaction(nil, func(txn Transaction) error {
			return db.Put(txn, true, rec0)
		})
		if err != nil {
			t.Error("Put failed:", err)
		}

		rec1 := &NumberedTestRecord{Key: rec0.Key}

		err = env.WithTransaction(nil, func(txn Transaction) (err error) {
			err = db.Del(txn, rec0)
			if err != nil {
				return
			}

			err = db.Get(txn, false, rec1)

			return
		})
		if err == nil {
			t.Error("Illegal del+get succeeded:", rec1)
		}

		err = env.WithTransaction(nil, func(txn Transaction) error {
			return db.Get(txn, false, rec1)
		})
		if err != nil {
			t.Error("Get failed:", err)
		}
		if *rec0.Val != *rec1.Val {
			t.Error("Retrieved value mismatch:", rec0, rec1)
		}
	})
}
