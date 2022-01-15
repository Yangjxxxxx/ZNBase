// Copyright 2019  The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/znbasedb/znbase/pkg/sql/tests"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestCommentOnConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE customers (
			id INT, 
			email STRING UNIQUE,
			num INT CHECK(num > 0),
			CONSTRAINT customers_pk_id PRIMARY KEY (id)
		);
		CREATE TABLE orders (
    		id INT ,
    		email STRING UNIQUE,
			num INT CHECK(num > 0),
			CONSTRAINT orders_pk_id PRIMARY KEY (id),
    		customer INT NOT NULL REFERENCES customers (id),
    		orderTotal DECIMAL(9,2),
    		INDEX (customer)
  		);
	`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		exec   string
		query  string
		expect gosql.NullString
	}{
		{
			`COMMENT ON CONSTRAINT orders_pk_id ON orders IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_pk_id';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`TRUNCATE orders`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_pk_id';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT orders_pk_id ON orders IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_pk_id';`,
			gosql.NullString{Valid: false},
		},

		{
			`COMMENT ON CONSTRAINT fk_customer_ref_customers ON orders IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='fk_customer_ref_customers';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`TRUNCATE orders`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='fk_customer_ref_customers';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT fk_customer_ref_customers ON orders IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='fk_customer_ref_customers';`,
			gosql.NullString{Valid: false},
		},

		{
			`COMMENT ON CONSTRAINT orders_email_key ON orders IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_email_key';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`TRUNCATE orders`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_email_key';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT orders_email_key ON orders IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_email_key';`,
			gosql.NullString{Valid: false},
		},

		{
			`COMMENT ON CONSTRAINT orders_check_num ON orders IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_check_num';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`TRUNCATE orders`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_check_num';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT orders_check_num ON orders IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='orders_check_num';`,
			gosql.NullString{Valid: false},
		},

		{
			`COMMENT ON CONSTRAINT customers_pk_id ON customers IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='customers_pk_id';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT customers_pk_id ON customers IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='customers_pk_id';`,
			gosql.NullString{Valid: false},
		},

		{
			`COMMENT ON CONSTRAINT customers_email_key ON customers IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='customers_email_key';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT customers_email_key ON customers IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='customers_email_key';`,
			gosql.NullString{Valid: false},
		},

		{
			`COMMENT ON CONSTRAINT customers_check_num ON customers IS 'constraint_comment'`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='customers_check_num';`,
			gosql.NullString{String: `constraint_comment`, Valid: true},
		},
		{
			`COMMENT ON CONSTRAINT customers_check_num ON customers IS NULL`,
			`SELECT obj_description(oid) from pg_constraint WHERE conname='customers_check_num';`,
			gosql.NullString{Valid: false},
		},
	}

	for _, tc := range testCases {
		if _, err := db.Exec(tc.exec); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(tc.query)
		var comment gosql.NullString
		if err := row.Scan(&comment); err != nil {
			t.Fatal(err)
		}
		if tc.expect != comment {
			t.Fatalf("expected comment %v, got %v", tc.expect, comment)
		}
	}
}

func TestCommentOnConstraintWhenDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (num INT, CHECK(num > 0));
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON CONSTRAINT t_check_num ON t IS 'constraint_comment'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`DROP TABLE t`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remain")
	}
}

func TestCommentOnConstraintWhenDropConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (num INT, CHECK(num > 0));
	`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`COMMENT ON CONSTRAINT t_check_num ON t IS 'constraint_comment'`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`ALTER TABLE t DROP CONSTRAINT t_check_num`); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
	var comment string
	err := row.Scan(&comment)
	if err != gosql.ErrNoRows {
		if err != nil {
			t.Fatal(err)
		}

		t.Fatal("comment remain")
	}
}
