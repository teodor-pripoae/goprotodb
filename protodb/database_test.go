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
	"os"
	"testing"
)

// Run an action with a database that is removed afterwards.
func withDb(t *testing.T, dbtype DatabaseType, action func(Database)) {
	db, err := OpenDatabase(NoEnvironment, NoTransaction, "test.db", &DatabaseConfig{
		Create: true,
		Type:   dbtype,
	})
	if err == nil {
		defer os.Remove("test.db")
	} else {
		t.Fatal("Failed to open database:", err)
	}

	action(db)

	err = db.Close()
	if err != nil {
		t.Error("Failed to close database:", err)
	}
}

// Run an action with an environment and a transactional database that
// are removed afterwards.
func withEnvDb(t *testing.T, dbtype DatabaseType, action func(Environment, Database)) {
	err := os.Mkdir("test.env", 0755)
	if err == nil {
		defer os.RemoveAll("test.env")
	} else {
		t.Fatal("Failed to create environment home:", err)
	}

	env, err := OpenEnvironment("test.env", &EnvironmentConfig{
		Create:        true,
		Transactional: true,
	})
	if err != nil {
		t.Fatal("Failed to open environment:", err)
	}

	var db Database
	err = env.WithTransaction(nil, func(txn Transaction) error {
		db, err = OpenDatabase(env, txn, "test.db", &DatabaseConfig{
			Create: true,
			Type:   dbtype,
		})
		return err
	})
	if err != nil {
		t.Fatal("Failed to open database:", err)
	}

	action(env, db)

	err = db.Close()
	if err != nil {
		t.Error("Failed to close database:", err)
	}

	err = env.Close()
	if err != nil {
		t.Error("Failed to close environment:", err)
	}
}
