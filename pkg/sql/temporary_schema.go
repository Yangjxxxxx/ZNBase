// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// temporarySchemaName returns the session specific temporary schema name given
// the sessionID. When the session creates a temporary object for the first
// time, it must create a schema with the name returned by this function.
func temporarySchemaName(sessionID ClusterWideID) string {
	return fmt.Sprintf("pg_temp_%d_%d", sessionID.Hi, sessionID.Lo)
}

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(
	ctx context.Context, db *client.DB, ie tree.SessionBoundInternalExecutor, sessionID ClusterWideID,
) error {
	tempSchemaName := temporarySchemaName(sessionID)
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// We are going to read all database descriptor IDs, then for each database
		// we will drop all the objects under the temporary schema.
		dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, txn)
		if err != nil {
			return err
		}
		for _, id := range dbIDs {

			databaseDesc, err := sqlbase.GetDatabaseDescFromID(ctx, txn, id)
			if err != nil {
				return err
			}
			for _, schemaDesc := range databaseDesc.Schemas {
				if strings.Compare(schemaDesc.Name, tempSchemaName) == 0 {
					var query strings.Builder
					query.WriteString("DROP ")
					query.WriteString("schema ")
					query.WriteString(databaseDesc.Name + ".")
					query.WriteString(tempSchemaName)
					query.WriteString(" CASCADE")
					_, err = ie.Query(ctx, "delete-temp-schema", txn, query.String())
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}
