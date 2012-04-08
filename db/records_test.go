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

import "code.google.com/p/goprotobuf/proto"

func (rec *TestRecord) RecordKey() interface{} {
	if rec.Key == nil {
		rec.Key = &TestRecord_Key{Val: proto.String("")}
	}
	return rec.Key
}

func (rec *TestRecord) RecordWithoutKey() interface{} {
	dup := new(TestRecord)
	*dup = *rec
	dup.Key = nil
	return dup
}

func (rec *NumberedTestRecord) RecordKey() interface{} {
	if rec.Key == nil {
		rec.Key = proto.Uint32(0)
	}
	return rec.Key
}

func (rec *NumberedTestRecord) RecordWithoutKey() interface{} {
	dup := new(NumberedTestRecord)
	*dup = *rec
	dup.Key = nil
	return dup
}
