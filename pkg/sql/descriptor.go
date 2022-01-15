// Copyright 2016  The Cockroach Authors.
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

package sql

import (
	"context"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

//
// This file contains routines for low-level access to stored
// descriptors.
//
// For higher levels in the SQL layer, these interface are likely not
// suitable; consider instead schema_accessors.go and resolver.go.
//

var (
	errEmptyDatabaseName = pgerror.NewError(pgcode.Syntax, "empty database name")
	errNoDatabase        = pgerror.NewError(pgcode.InvalidName, "no database specified")
	errNoSchema          = pgerror.NewError(pgcode.InvalidName, "no schema specified")
	errNoTable           = pgerror.NewError(pgcode.InvalidName, "no table specified")
	errNoView            = pgerror.NewError(pgcode.InvalidName, "no view specified")
	errNoSequence        = pgerror.NewError(pgcode.InvalidName, "no sequence specified")
	errNoMatch           = pgerror.NewError(pgcode.UndefinedObject, "no object matched")
	// udr
	errEmptyTriggerName      = pgerror.NewError(pgcode.Syntax, "empty trigger name")
	errEmptyFunctionName     = pgerror.NewError(pgcode.Syntax, "empty function name")
	errOneLetterFunctionName = pgerror.NewError(pgcode.InvalidName, "no sequence specified")
	errIllegalFunctionName   = pgerror.NewError(pgcode.InvalidName, "Invalid function name")
)

// DefaultUserDBs is a set of the databases which are present in a new cluster.
var DefaultUserDBs = map[string]struct{}{
	sessiondata.DefaultDatabaseName: {},
	sessiondata.PgDatabaseName:      {},
}

// MaxDefaultDescriptorID is the maximum ID of a descriptor that exists in a
// new cluster.
var MaxDefaultDescriptorID = keys.MaxReservedDescID + sqlbase.ID(len(DefaultUserDBs)) + 2

// GenerateUniqueDescID returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func GenerateUniqueDescID(ctx context.Context, db *client.DB) (sqlbase.ID, error) {
	// Increment unique descriptor counter.
	newVal, err := client.IncrementValRetryable(ctx, db, keys.DescIDGenerator, 1)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	return sqlbase.ID(newVal - 1), nil
}

// createdatabase takes Database descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createDatabase implements the DatabaseDescEditor interface.
func (p *planner) createDatabase(
	ctx context.Context, database *tree.CreateDatabase,
) (*sqlbase.DatabaseDescriptor, bool, error) {
	dbName := string(database.Name)
	plainKey := databaseKey{dbName}
	idKey := plainKey.Key()

	if exists, err := descExists(ctx, p.txn, idKey); err == nil && exists {
		if database.IfNotExists {
			// Noop.
			return nil, false, nil
		}
		return nil, false, sqlbase.NewDatabaseAlreadyExistsError(plainKey.Name())
	} else if err != nil {
		return nil, false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return nil, false, err
	}

	desc := NewInitialDatabaseDescriptor(id, string(database.Name), p.User())

	return desc, true, p.createDescriptorWithID(ctx, idKey, id, desc, nil)
}

func descExists(ctx context.Context, txn *client.Txn, idKey roachpb.Key) (bool, error) {
	// Check whether idKey exists.
	gr, err := txn.Get(ctx, idKey)
	if err != nil {
		return false, err
	}
	return gr.Exists(), nil
}

func (p *planner) createDescriptorWithID(
	ctx context.Context,
	idKey roachpb.Key,
	id sqlbase.ID,
	descriptor sqlbase.DescriptorProto,
	st *cluster.Settings,
) error {
	descriptor.SetID(id)
	// TODO(pmattis): The error currently returned below is likely going to be
	// difficult to interpret.
	//
	// TODO(pmattis): Need to handle if-not-exists here as well.
	//
	// TODO(pmattis): This is writing the namespace and descriptor table entries,
	// but not going through the normal INSERT logic and not performing a precise
	// mimicry. In particular, we're only writing a single key per table, while
	// perfect mimicry would involve writing a sentinel key for each row as well.
	descKey := sqlbase.MakeDescMetadataKey(descriptor.GetID())

	b := &client.Batch{}
	descID := descriptor.GetID()
	descDesc := sqlbase.WrapDescriptor(descriptor)
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", idKey, descID)
		log.VEventf(ctx, 2, "CPut %s -> %s", descKey, descDesc)
	}
	b.CPut(idKey, descID, nil)
	b.CPut(descKey, descDesc, nil)

	mutDesc, isTable := descriptor.(*sqlbase.MutableTableDescriptor)
	if isTable {
		if err := mutDesc.ValidateTable(st); err != nil {
			return err
		}
		if err := p.Tables().addUncommittedTable(*mutDesc); err != nil {
			return err
		}
	}
	funcDesc, isFunction := descriptor.(*sqlbase.FunctionDescriptor)
	if isFunction {
		mutFuncDesc := sqlbase.NewMutableCreatedFunctionDescriptor(*funcDesc)
		if err := mutFuncDesc.ValidateFunction(st); err != nil {
			return err
		}
		if err := p.Tables().addUncommittedFunction(*mutFuncDesc); err != nil {
			return err
		}
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return err
	}
	if isTable && mutDesc.Adding() {
		p.queueSchemaChange(mutDesc.TableDesc(), sqlbase.InvalidMutationID)
	}
	return nil
}

// getSchemaDescriptorID looks up the ID for schemaKey.
// InvalidID is returned if the name cannot be resolved.
func getSchemaDescriptorID(
	ctx context.Context, txn *client.Txn, plainKey schemaKey,
) (sqlbase.ID, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up schema descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !gr.Exists() {
		return sqlbase.InvalidID, nil
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// getFunctionDescriptorID looks up the ID for schemaKey.
// InvalidID is returned if the name cannot be resolved.
func getFunctionDescriptorID(
	ctx context.Context, txn *client.Txn, plainKey functionKey,
) (sqlbase.ID, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up function descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !gr.Exists() {
		return sqlbase.InvalidID, nil
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// getDescriptorID looks up the ID for plainKey.
// InvalidID is returned if the name cannot be resolved.
func getDescriptorID(
	ctx context.Context, txn *client.Txn, plainKey sqlbase.DescriptorKey,
) (sqlbase.ID, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up descriptor ID for name key %q", key)
	gr, err := txn.Get(ctx, key)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if !gr.Exists() {
		return sqlbase.InvalidID, nil
	}
	return sqlbase.ID(gr.ValueInt()), nil
}

// getFunctionGroupIDs lookup function group fro plainKey.
// InvalidID is returned if name cannot be resolved.
func getFunctionGroupIDs(
	ctx context.Context, txn *client.Txn, plainKey sqlbase.DescriptorKey,
) ([]sqlbase.ID, error) {
	key := plainKey.Key()
	log.Eventf(ctx, "looking up descriptor IDs for name key %q", key)
	kvs, err := txn.Scan(ctx, key, key.PrefixEnd(), 0)
	if err != nil {
		return []sqlbase.ID{sqlbase.InvalidID}, err
	}
	IDs := make([]sqlbase.ID, 0)
	for _, gr := range kvs {
		IDs = append(IDs, sqlbase.ID(gr.ValueInt()))
	}
	return IDs, nil
}

// getDescriptorByID looks up the descriptor for `id`, validates it,
// and unmarshals it into `descriptor`.
//
// In most cases you'll want to use wrappers: `getDatabaseDescByID` or
// `getTableDescByID`.
func getDescriptorByID(
	ctx context.Context, txn *client.Txn, id sqlbase.ID, descriptor sqlbase.DescriptorProto,
) error {
	log.Eventf(ctx, "fetching descriptor with ID %d", id)
	descKey := sqlbase.MakeDescMetadataKey(id)
	desc := &sqlbase.Descriptor{}
	ts, err := txn.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return err
	}

	switch t := descriptor.(type) {
	case *sqlbase.TableDescriptor:
		table := desc.Table(ts)
		if table == nil {
			return errors.Errorf("%q is not a table", desc.String())
		}
		table.MaybeFillInDescriptor()

		if err := table.Validate(ctx, txn, nil /* clusterVersion */); err != nil {
			return err
		}
		*t = *table
	case *sqlbase.SchemaDescriptor:
		schema := desc.GetSchema()
		if schema == nil {
			return errors.Errorf("%q is not a schema", desc.String())
		}

		if err := schema.Validate(); err != nil {
			return err
		}
		*t = *schema
	case *sqlbase.DatabaseDescriptor:
		database := desc.GetDatabase()
		if database == nil {
			return errors.Errorf("%q is not a database", desc.String())
		}

		if err := database.Validate(); err != nil {
			return err
		}
		*t = *database
	case *sqlbase.FunctionDescriptor:
		function := desc.GetFunction()
		if function == nil {
			return errors.Errorf("%q is not function", desc.String())
		}
		if err := function.Validate(); err != nil {
			return err
		}
		*t = *function

	}
	return nil
}

// IsDefaultCreatedDescriptor returns whether or not a given descriptor ID is
// present at the time of starting a cluster.
func IsDefaultCreatedDescriptor(descID sqlbase.ID) bool {
	return descID <= MaxDefaultDescriptorID
}

// CountUserDescriptors returns the number of descriptors present that were
// created by the user (i.e. not present when the cluster started).
func CountUserDescriptors(ctx context.Context, txn *client.Txn) (int, bool, error) {
	var noWaitForGC bool
	allDescs, err := GetAllDescriptors(ctx, txn)
	if err != nil {
		return 0, noWaitForGC, err
	}

	count := 0
	for _, desc := range allDescs {
		if !IsDefaultCreatedDescriptor(desc.GetID()) {
			if checkDescWaitForGC(desc) {
				noWaitForGC = true
			}
			count++
		}
	}
	return count, noWaitForGC, nil
}
func checkDescWaitForGC(desc sqlbase.DescriptorProto) bool {
	var noWaitForGC bool
	switch t := desc.(type) {
	case *sqlbase.TableDescriptor:
		if t.State == sqlbase.TableDescriptor_PUBLIC {
			noWaitForGC = true
		}
	case *sqlbase.DatabaseDescriptor:
		noWaitForGC = true
	case *sqlbase.SchemaDescriptor:
		noWaitForGC = true
	}
	return noWaitForGC
}

// GetAllDescriptors looks up and returns all available descriptors.
func GetAllDescriptors(ctx context.Context, txn *client.Txn) ([]sqlbase.DescriptorProto, error) {
	log.Eventf(ctx, "fetching all descriptors")
	descsKey := sqlbase.MakeAllDescsMetadataKey()
	kvs, err := txn.Scan(ctx, descsKey, descsKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	descs := make([]sqlbase.DescriptorProto, len(kvs))

	var tables []*sqlbase.Descriptor
	tblToIndexMap := make(map[uint64]int)
	for i, kv := range kvs {
		desc := &sqlbase.Descriptor{}
		if err := kv.ValueProto(desc); err != nil {
			return nil, err
		}
		switch t := desc.Union.(type) {
		case *sqlbase.Descriptor_Table:
			tables = append(tables, desc)
			tblToIndexMap[uint64(desc.GetID())] = i
		case *sqlbase.Descriptor_Database:
			descs[i] = desc.GetDatabase()
		case *sqlbase.Descriptor_Schema:
			descs[i] = desc.GetSchema()
		case *sqlbase.Descriptor_Function:
			descs[i] = desc.GetFunction()
		default:
			return nil, errors.Errorf("Descriptor.Union has unexpected type %T", t)
		}
	}
	b := txn.NewBatch()
	for _, tblDesc := range tables {
		descKey := sqlbase.MakeDescMetadataKey(tblDesc.GetID())
		b.AddRawRequest(roachpb.NewGet(descKey))
	}

	if err = txn.Run(ctx, b); err != nil {
		return nil, err
	}
	for i, tblDesc := range tables {
		descs[tblToIndexMap[uint64(tblDesc.GetID())]] = tblDesc.Table(b.RawResponse().Responses[i].Value.(*roachpb.ResponseUnion_Get).Get.Value.Timestamp)
	}
	//for i, kv := range kvs {
	//	desc := &sqlbase.Descriptor{}
	//	if err := kv.ValueProto(desc); err != nil {
	//		return nil, err
	//	}
	//	switch t := desc.Union.(type) {
	//	case *sqlbase.Descriptor_Table:
	//		descKey := sqlbase.MakeDescMetadataKey(desc.GetID())
	//		ts, err := txn.GetProtoTs(ctx, descKey, desc)
	//		if err != nil {
	//			return nil, err
	//		}
	//		descs[i] = desc.Table(ts)
	//	case *sqlbase.Descriptor_Database:
	//		descs[i] = desc.GetDatabase()
	//	case *sqlbase.Descriptor_Schema:
	//		descs[i] = desc.GetSchema()
	//	case *sqlbase.Descriptor_Function:
	//		descs[i] = desc.GetFunction()
	//	default:
	//		return nil, errors.Errorf("Descriptor.Union has unexpected type %T", t)
	//	}
	//}
	return descs, nil
}

// GetAllDatabaseDescriptorIDs looks up and returns all available database
// descriptor IDs.
func GetAllDatabaseDescriptorIDs(ctx context.Context, txn *client.Txn) ([]sqlbase.ID, error) {
	log.Eventf(ctx, "fetching all database descriptor IDs")
	nameKey := sqlbase.NewDatabaseKey("" /* name */).Key()
	kvs, err := txn.Scan(ctx, nameKey, nameKey.PrefixEnd(), 0 /*maxRows */)
	if err != nil {
		return nil, err
	}
	// See the comment in physical_schema_accessors.go,
	// func (a UncachedPhysicalAccessor) GetObjectNames. Same concept
	// applies here.

	descIDs := make([]sqlbase.ID, 0, len(kvs))
	alreadySeen := make(map[sqlbase.ID]bool)
	for _, kv := range kvs {
		ID := sqlbase.ID(kv.ValueInt())
		if alreadySeen[ID] {
			continue
		}
		alreadySeen[ID] = true
		descIDs = append(descIDs, ID)
	}
	return descIDs, nil
}

// WriteDescToBatch adds a Put command writing a descriptor proto to the
// descriptors table. It writes the descriptor desc at the id descID. If kvTrace
// is enabled, it will log an event explaining the put that was performed.
func WriteDescToBatch(
	ctx context.Context,
	kvTrace bool,
	s *cluster.Settings,
	b *client.Batch,
	descID sqlbase.ID,
	desc sqlbase.DescriptorProto,
) (err error) {
	descKey := sqlbase.MakeDescMetadataKey(descID)
	descDesc := sqlbase.WrapDescriptor(desc)
	if kvTrace {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descDesc)
	}
	b.Put(descKey, descDesc)
	return nil
}
