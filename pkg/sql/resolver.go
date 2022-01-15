// Copyright 2018  The Cockroach Authors.
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
	"fmt"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/row"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// SchemaResolver abstracts the interfaces needed from the logical
// planner to perform name resolution below.
//
// We use an interface instead of passing *planner directly to make
// the resolution methods able to work even when we evolve the code to
// use a different plan builder.
// TODO(rytaft,andyk): study and reuse this.
type SchemaResolver interface {
	tree.TableNameExistingResolver
	tree.TableNameTargetResolver

	Txn() *client.Txn
	LogicalSchemaAccessor() SchemaAccessor
	CurrentDatabase() string
	CurrentSearchPath() sessiondata.SearchPath
	CommonLookupFlags(required bool) CommonLookupFlags
	ObjectLookupFlags(required bool, requireMutable bool) ObjectLookupFlags
	LookupTableByID(ctx context.Context, id sqlbase.ID) (row.TableEntry, error)
}

var _ SchemaResolver = &planner{}

var errNoPrimaryKey = errors.New("requested table does not have a primary key")

// LogicalSchema encapsulates the interfaces needed to be able to both look up
// schema objects and also resolve permissions on them.
type LogicalSchema interface {
	SchemaResolver
	AuthorizationAccessor
}

var _ LogicalSchema = &planner{}

// ResolveUncachedDatabaseByName looks up a database name from the store.
func (p *planner) ResolveUncachedDatabaseByName(
	ctx context.Context, dbName string, required bool,
) (res *UncachedDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, err = p.LogicalSchemaAccessor().GetDatabaseDesc(ctx, p.txn, dbName,
			p.CommonLookupFlags(required))
	})
	return res, err
}

// GetObjectNames retrieves the names of all objects in the target database/schema.
func GetObjectNames(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	dbDesc *DatabaseDescriptor,
	scNames []string,
	explicitPrefix bool,
) (res TableNames, err error) {
	lsa := sc.LogicalSchemaAccessor()
	for _, scName := range scNames {
		tbNames, err := lsa.GetObjectNames(ctx, txn, dbDesc, scName,
			DatabaseListFlags{
				CommonLookupFlags: sc.CommonLookupFlags(true /*required*/),
				explicitPrefix:    explicitPrefix,
			})
		if err != nil {
			return nil, err
		}
		res = append(res, tbNames...)
	}
	return res, err
}

// ResolveExistingObject looks up an existing object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveExistingObject(
	ctx context.Context, sc SchemaResolver, tn *ObjectName, required bool, requiredType requiredType,
) (res *ImmutableTableDescriptor, err error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, required, false /* requiredMutable */, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*ImmutableTableDescriptor), nil
}

// ResolveMutableExistingObject looks up an existing mutable object.
// If required is true, an error is returned if the object does not exist.
// Optionally, if a desired descriptor type is specified, that type is checked.
//
// The object name is modified in-place with the result of the name
// resolution, if successful. It is not modified in case of error or
// if no object is found.
func ResolveMutableExistingObject(
	ctx context.Context, sc SchemaResolver, tn *ObjectName, required bool, requiredType requiredType,
) (res tree.NameResolutionResult, err error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, tn, required, true /* requiredMutable */, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	return desc.(*MutableTableDescriptor), nil
}

func resolveExistingObjectImpl(
	ctx context.Context,
	sc SchemaResolver,
	tn *ObjectName,
	required bool,
	requiredMutable bool,
	requiredType requiredType,
) (res tree.NameResolutionResult, err error) {
	found, descI, err := tn.ResolveExisting(ctx, sc, requiredMutable, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		if required {
			if requiredType == requireFunctionDesc {
				return nil, sqlbase.NewUndefinedFunctionError(tn.Table())
			}
			return nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return nil, nil
	}
	obj := descI.(ObjectDescriptor)

	goodType := true
	switch requiredType {
	case requireTableDesc:
		goodType = obj.TableDesc().IsTable()
	case requireViewDesc:
		goodType = obj.TableDesc().IsView()
	case requireTableOrViewDesc:
		goodType = obj.TableDesc().IsTable() || obj.TableDesc().IsView()
	case requireTableOrMviewDesc:
		goodType = obj.TableDesc().IsTable() || obj.TableDesc().MaterializedView()
	case requireSequenceDesc:
		goodType = obj.TableDesc().IsSequence()
	case requireTableOrViewOrSequenceDesc:
		goodType = obj.TableDesc().IsTable() || obj.TableDesc().IsView() || obj.TableDesc().IsSequence()
	case requireFunctionDesc:
		if requiredMutable {
			functionDesc, ok := descI.(*sqlbase.MutableFunctionDescriptorGroup)
			if !ok {
				return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
			}
			return functionDesc, nil
		}
		functionDesc, ok := descI.(*sqlbase.ImmutableFunctionDescriptor)
		if !ok {
			return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
		}
		return functionDesc, nil
	}
	if !goodType {
		return nil, sqlbase.NewWrongObjectTypeError(tn, requiredTypeNames[requiredType])
	}

	// If the table does not have a primary key, return an error
	// that the requested descriptor is invalid for use.
	if !required &&
		obj.TableDesc().IsTable() &&
		!obj.TableDesc().HasPrimaryKey() {
		return nil, errNoPrimaryKey
	}

	if requiredMutable {
		return descI.(*MutableTableDescriptor), nil
	}

	return descI.(*ImmutableTableDescriptor), nil
}

// ResolveMutableTableDescriptorExAllowNoPrimaryKey performs the
// same logic as ResolveMutableTableDescriptorEx but allows for
// the resolved table to not have a primary key.
func (p *planner) ResolveMutableTableDescriptorExAllowNoPrimaryKey(
	ctx context.Context,
	sc SchemaResolver,
	tn tree.TableName,
	required bool,
	requiredMutable bool,
	requiredType requiredType,
) (*MutableTableDescriptor, error) {
	desc, err := resolveExistingObjectImpl(ctx, sc, &tn, required, requiredMutable, requiredType)
	if err != nil || desc == nil {
		return nil, err
	}
	// name.SetAnnotation(&p.semaCtx.Annotations, &tn)
	return desc.(*MutableTableDescriptor), nil
}

// runWithOptions sets the provided resolution flags for the
// duration of the call of the passed argument fn.
//
// This is meant to be used like this (for example):
//
// var someVar T
// var err error
// p.runWithOptions(resolveFlags{skipCache: true}, func() {
//    someVar, err = ResolveExistingObject(ctx, p, ...)
// })
// if err != nil { ... }
// use(someVar)
func (p *planner) runWithOptions(flags resolveFlags, fn func()) {
	if flags.skipCache {
		defer func(prev bool) { p.avoidCachedDescriptors = prev }(p.avoidCachedDescriptors)
		p.avoidCachedDescriptors = true
	}
	fn()
}

type resolveFlags struct {
	skipCache bool
}

func (p *planner) ResolveMutableTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType requiredType,
) (table *MutableTableDescriptor, err error) {
	objects, err := ResolveMutableExistingObject(ctx, p, tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	if mutableTable, ok := objects.(*MutableTableDescriptor); ok {
		return mutableTable, nil
	}
	if required {
		return nil, sqlbase.NewUndefinedRelationError(tn)
	}
	return nil, nil
	// return ResolveMutableExistingObject(ctx, p, tn, required, requiredType)
}

// ResolveMutableTableDescriptorEx resolves a type descriptor for mutable access.
func (p *planner) ResolveMutableTableDescriptorEx(
	ctx context.Context, name *tree.UnresolvedObjectName, required bool, requiredType requiredType,
) (*sqlbase.MutableTableDescriptor, error) {
	tn := name.ToTableName()
	objects, err := ResolveMutableExistingObject(ctx, p, &tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	mutableTable := objects.(*MutableTableDescriptor)

	if mutableTable != nil {
		//TODO: 是否需要实现canResolveDescUnderSchema
		// Ensure that the user can access the target schema.
		//if err := p.canResolveDescUnderSchema(ctx, mutableTable.GetParentSchemaID(), mutableTable); err != nil {
		//	return nil, err
		//}
	}
	return mutableTable, nil
}

func (p *planner) ResolveMutableFuncDescriptorGrp(
	ctx context.Context, tn *ObjectName, required bool, requiredType requiredType,
) (funcGroup *sqlbase.MutableFunctionDescriptorGroup, err error) {
	funcGrp, err := resolveExistingObjectImpl(ctx, p, tn, true, true, requireFunctionDesc)
	//objects, err := ResolveMutableExistingObject(ctx, p, tn, required, requiredType)
	if err != nil {
		return nil, err
	}
	if mutableFunction, ok := funcGrp.(*sqlbase.MutableFunctionDescriptorGroup); ok {
		return mutableFunction, nil
	}
	if required {
		return nil, sqlbase.NewUndefinedFunctionError(tn.Table())
	}
	return nil, nil
}

func (p *planner) ResolveUncachedTableDescriptor(
	ctx context.Context, tn *ObjectName, required bool, requiredType requiredType,
) (table *ImmutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		table, err = ResolveExistingObject(ctx, p, tn, required, requiredType)
	})
	return table, err
}

// ResolveTargetObject determines a valid target path for an object
// that may not exist yet. It returns the descriptor for the database
// where the target object lives.
//
// The object name is modified in-place with the result of the name
// resolution.
func ResolveTargetObject(
	ctx context.Context, sc SchemaResolver, tn *ObjectName,
) (res *DatabaseDescriptor, err error) {
	found, descI, err := tn.ResolveTarget(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, pgerror.NewErrorf(pgcode.InvalidSchemaName,
			"cannot create %q because the target database or schema does not exist",
			tree.ErrString(tn)).SetHintf("verify that the current database and search_path are valid and/or the target database exists")
	}

	return descI.(*DatabaseDescriptor), nil
}

func (p *planner) ResolveUncachedDatabase(
	ctx context.Context, tn *ObjectName,
) (res *UncachedDatabaseDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		res, err = ResolveTargetObject(ctx, p, tn)
	})
	return res, err
}

// requiredType can be passed to the ResolveExistingObject function to
// require the returned descriptor to be of a specific type.
type requiredType int

const (
	anyDescType requiredType = iota
	requireTableDesc
	requireViewDesc
	requireTableOrViewDesc
	requireSequenceDesc
	requireFunctionDesc
	requireTableOrViewOrSequenceDesc
	requireTableOrMviewDesc
)

var requiredTypeNames = [...]string{
	requireTableDesc:       "table",
	requireViewDesc:        "view",
	requireTableOrViewDesc: "table or view",
	requireSequenceDesc:    "sequence",

	requireFunctionDesc:              "function or procedure",
	requireTableOrViewOrSequenceDesc: "table or view or sequence",
	requireTableOrMviewDesc:          "table or materialized view",
}

// LookupSchema implements the tree.TableNameTargetResolver interface.
func (p *planner) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	sc := p.LogicalSchemaAccessor()
	dbDesc, err := sc.GetDatabaseDesc(ctx, p.txn, dbName, p.CommonLookupFlags(false /*required*/))
	if err != nil || dbDesc == nil {
		return false, nil, err
	}
	return sc.IsValidSchema(ctx, p.txn, dbDesc, scName, p.CommonLookupFlags(true)), dbDesc, nil
}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (p *planner) LookupObject(
	ctx context.Context, requireMutable bool, requireFunction bool, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	sc := p.LogicalSchemaAccessor()
	p.tableName = tree.MakeTableNameWithSchema(tree.Name(dbName), tree.Name(scName), tree.Name(tbName))
	flags := p.ObjectLookupFlags(false /*required*/, requireMutable)
	flags.requireFunctionDesc = requireFunction
	p.tableTrigger, err = sc.GetObjectDesc(ctx, p.txn, &p.tableName, flags)
	return p.tableTrigger != nil, p.tableTrigger, err
}

func (p *planner) CommonLookupFlags(required bool) CommonLookupFlags {
	return CommonLookupFlags{
		required:    required,
		avoidCached: p.avoidCachedDescriptors,
	}
}

func (p *planner) ObjectLookupFlags(required, requireMutable bool) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: p.CommonLookupFlags(required),
		requireMutable:    requireMutable,
	}
}

// getDescriptorsFromTargetList fetches the descriptors for the targets.
func getDescriptorsFromTargetList(
	ctx context.Context, p *planner, targets tree.TargetList,
) ([]sqlbase.DescriptorProto, error) {
	if targets.Databases != nil {
		if len(targets.Databases) == 0 {
			return nil, errNoDatabase
		}
		descs := make([]sqlbase.DescriptorProto, 0, len(targets.Databases))
		for _, database := range targets.Databases {
			descriptor, err := p.ResolveUncachedDatabaseByName(ctx, string(database), true /*required*/)
			if err != nil {
				return nil, err
			}
			descs = append(descs, descriptor)
		}
		if len(descs) == 0 {
			return nil, errNoMatch
		}
		return descs, nil
	} else if targets.Schemas != nil {
		if len(targets.Schemas) == 0 {
			return nil, errNoSchema
		}
		database := p.CurrentDatabase()
		scDescs := make([]sqlbase.DescriptorProto, 0, len(targets.Schemas))
		for _, schema := range targets.Schemas {
			// check if the schema is secondary structure
			if schema.IsExplicitCatalog() {
				database = schema.Catalog()
			}
			dbDesc, err := getDatabaseDesc(ctx, p.txn, database)
			if err != nil {
				return nil, err
			}
			descriptor, err := dbDesc.GetSchemaByName(schema.Schema())
			if err != nil {
				return nil, err
			}
			scDescs = append(scDescs, descriptor)
		}
		if len(scDescs) == 0 {
			return nil, errNoMatch
		}
		return scDescs, nil
	} else if targets.Tables != nil {
		descs, err := getRelationDescriptors(ctx, p, targets.Tables, requireTableDesc)
		if err != nil {
			return nil, err
		}
		if targets.Columns != nil {
			colDescs := make([]sqlbase.DescriptorProto, 0, len(targets.Columns))
			for _, column := range targets.Columns {
				for _, desc := range descs {
					tblDesc := desc.(*MutableTableDescriptor)
					descriptor, err := tblDesc.GetColumnByName(string(column))
					if err != nil {
						return nil, err
					}
					if descriptor.Privileges == nil {
						descriptor.Privileges = sqlbase.InheritFromTablePrivileges(tblDesc.Privileges)
						descriptor.ParentID = tblDesc.ID
					}
					colDescs = append(colDescs, descriptor)
				}
			}
			return colDescs, nil
		}
		return descs, nil
	} else if targets.Views != nil {
		descs, err := getRelationDescriptors(ctx, p, targets.Views, requireViewDesc)
		if err != nil {
			return nil, err
		}
		if targets.Columns != nil {
			colDescs := make([]sqlbase.DescriptorProto, 0, len(targets.Columns))
			for _, column := range targets.Columns {
				for _, desc := range descs {
					tblDesc := desc.(*MutableTableDescriptor)
					descriptor, err := tblDesc.GetColumnByName(string(column))
					if err != nil {
						return nil, err
					}
					colDescs = append(colDescs, descriptor)
				}
			}
			return colDescs, nil
		}
		return descs, err
	} else if targets.Sequences != nil {
		descs, err := getRelationDescriptors(ctx, p, targets.Sequences, requireSequenceDesc)
		if err != nil {
			return nil, err
		}
		return descs, nil
	}
	// only case is targets.Function
	descs, err := getFunctionDescriptors(ctx, p, targets.Function)
	if err != nil {
		return nil, err
	}
	return descs, nil
}

func getRelationDescriptors(
	ctx context.Context, p *planner, relations tree.TablePatterns, relationsType requiredType,
) ([]sqlbase.DescriptorProto, error) {
	if len(relations) == 0 {
		switch relationsType {
		case requireTableDesc:
			return nil, errNoTable
		case requireViewDesc:
			return nil, errNoView
		case requireSequenceDesc:
			return nil, errNoSequence
		}
	}
	descs := make([]sqlbase.DescriptorProto, 0, len(relations))
	for _, relationTarget := range relations {
		getDescs, err := checkRelationType(ctx, p, len(relations), relationTarget, relationsType)
		if err != nil {
			return nil, err
		}
		descs = append(descs, getDescs...)
	}
	return descs, nil
}

func getFunctionDescriptors(
	ctx context.Context, p *planner, function tree.Function,
) ([]sqlbase.DescriptorProto, error) {
	paraStr, err := function.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return nil, err
	}
	keyName := function.FuncName.Table() + paraStr

	var database = ""
	var schema = ""
	if function.FuncName.ExplicitCatalog {
		database = function.FuncName.Catalog()
	} else {
		database = p.CurrentDatabase()
	}
	if function.FuncName.ExplicitSchema {
		schema = function.FuncName.Schema()
	} else {
		schema = p.CurrentSearchPath().String()
	}

	dbDesc, err := getDatabaseDesc(ctx, p.Txn(), database)
	if err != nil {
		return nil, err
	}
	desc, err := getFuncDesc(ctx, p, schema, dbDesc, keyName)
	if err != nil {
		return nil, err
	}
	descs := make([]sqlbase.DescriptorProto, 0, 1)
	descs = append(descs, desc)
	return descs, nil
}

// getQualifiedTableName returns the database-qualified name of the table
// or view represented by the provided descriptor. It is a sort of
// reverse of the Resolve() functions.
func (p *planner) getQualifiedTableName(
	ctx context.Context, desc *sqlbase.TableDescriptor,
) (string, error) {
	tbName, err := sqlbase.GetTableName(ctx, p.txn, desc.ParentID, desc.Name)
	if err != nil {
		return "", err
	}
	return tbName.String(), nil
}

// findTableContainingIndex returns the descriptor of a table
// containing the index of the given name.
// This is used by expandMutableIndexName().
//
// An error is returned if the index name is ambiguous (i.e. exists in
// multiple tables). If no table is found and requireTable is true, an
// error will be returned, otherwise the TableName and descriptor
// returned will be nil.
func findTableContainingIndex(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	dbName, scName string,
	idxName tree.UnrestrictedName,
	lookupFlags CommonLookupFlags,
) (result *tree.TableName, desc *MutableTableDescriptor, err error) {
	sa := sc.LogicalSchemaAccessor()
	dbDesc, err := sa.GetDatabaseDesc(ctx, txn, dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sa.GetObjectNames(ctx, txn, dbDesc, scName,
		DatabaseListFlags{CommonLookupFlags: lookupFlags, explicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	for i := range tns {
		tn := &tns[i]
		tableDescTemp, err := ResolveMutableExistingObject(ctx, sc, tn, true, anyDescType)
		if err != nil {
			// todo why err continue
			log.Error(ctx, err)
			continue //return nil, nil, err
		}
		tableDesc, ok := tableDescTemp.(*MutableTableDescriptor)
		if !ok {
			return nil, nil, sqlbase.NewUndefinedRelationError(tn)
		}
		if tableDesc == nil || !(tableDesc.IsTable() || tableDesc.MaterializedView()) {
			continue
		}

		_, dropped, err := tableDesc.FindIndexByName(string(idxName))
		if err != nil || dropped {
			// err is nil if the index does not exist on the table.
			continue
		}
		if result != nil {
			return nil, nil, pgerror.NewErrorf(pgcode.AmbiguousParameter,
				"index name %q is ambiguous (found in %s and %s)",
				idxName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.required {
		return nil, nil, pgerror.NewErrorf(pgcode.UndefinedObject,
			"index %q does not exist", idxName)
	}
	return result, desc, nil
}

// findTableContainingConstraint returns the descriptor of a table
// containing the constraint of the given name.
// This is used by expandMutableConstraintName().
//
// An error is returned if the constraint name is ambiguous (i.e. exists in
// multiple tables). If no table is found and requireTable is true, an
// error will be returned, otherwise the TableName and descriptor
// returned will be nil.
func findTableContainingConstraint(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	dbName, scName string,
	cstName tree.UnrestrictedName,
	lookupFlags CommonLookupFlags,
) (result *tree.TableName, desc *MutableTableDescriptor, err error) {
	sa := sc.LogicalSchemaAccessor()
	dbDesc, err := sa.GetDatabaseDesc(ctx, txn, dbName, lookupFlags)
	if dbDesc == nil || err != nil {
		return nil, nil, err
	}

	tns, err := sa.GetObjectNames(ctx, txn, dbDesc, scName,
		DatabaseListFlags{CommonLookupFlags: lookupFlags, explicitPrefix: true})
	if err != nil {
		return nil, nil, err
	}

	result = nil
	for i := range tns {
		tn := &tns[i]
		tableDescTemp, err := ResolveMutableExistingObject(ctx, sc, tn, true, anyDescType)
		if err != nil {
			return nil, nil, err
		}
		tableDesc, ok := tableDescTemp.(*MutableTableDescriptor)
		if !ok {
			return nil, nil, sqlbase.NewUndefinedRelationError(tn)
		}
		if tableDesc == nil || !tableDesc.IsTable() {
			continue
		}

		_, dropped, err := tableDesc.FindConstraintByName(ctx, txn, string(cstName))
		if err != nil || dropped {
			// err is nil if the constraint does not exist on the table.
			continue
		}
		if result != nil {
			return nil, nil, pgerror.NewErrorf(pgcode.AmbiguousParameter,
				"constraint name %q is ambiguous (found in %s and %s)",
				cstName, tn.String(), result.String())
		}
		result = tn
		desc = tableDesc
	}
	if result == nil && lookupFlags.required {
		return nil, nil, pgerror.NewErrorf(pgcode.UndefinedObject,
			"constraint %q does not exist", cstName)
	}
	return result, desc, nil
}

// expandMutableIndexName ensures that the index name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandMutableIndexName(
	ctx context.Context, p *planner, index *tree.TableIndexName, requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandIndexName(ctx, p.txn, p, index, requireTable)
	})
	return tn, desc, err
}

// expandMutableConstraintName ensures that the constraint name is qualified with a table
// name, and searches the table name if not yet specified.
//
// It returns the TableName of the underlying table for convenience.
// If no table is found and requireTable is true an error will be
// returned, otherwise the TableName returned will be nil.
//
// It *may* return the descriptor of the underlying table, depending
// on the lookup path. This can be used in the caller to avoid a 2nd
// lookup.
func expandMutableConstraintName(
	ctx context.Context, p *planner, constraint *tree.Name, table *tree.TableName, requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		tn, desc, err = expandConstraintName(ctx, p.txn, p, constraint, table, requireTable)
	})
	return tn, desc, err
}

func expandIndexName(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	index *tree.TableIndexName,
	requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = &index.Table
	if tn.Table() != "" {
		// The index and its table prefix must exist already. Resolve the table.
		descTemp, err := ResolveMutableExistingObject(ctx, sc, tn, requireTable, requireTableOrViewDesc)
		if err != nil {
			return nil, nil, err
		}
		if desc != nil && desc.IsView() && !desc.MaterializedView() {
			return nil, nil, pgerror.Newf(pgcode.WrongObjectType,
				"%q is not a table or materialized view", tn.Table())
		}
		desc, ok := descTemp.(*MutableTableDescriptor)
		if !ok {
			return nil, nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return tn, desc, nil
	}

	// On the first call to expandMutableIndexName(), index.Table.Table() is empty.
	// Once the table name is resolved for the index below, index.Table
	// references the table name.

	// Look up the table prefix.
	found, _, err := tn.TableNamePrefix.Resolve(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, nil, err
	}
	if !found {
		if requireTable {
			return nil, nil, pgerror.NewErrorf(pgcode.UndefinedObject,
				"schema or database was not found while searching index: %q",
				tree.ErrString(&index.Index)).SetHintf(
				"check the current database and search_path are valid")
		}
		return nil, nil, nil
	}

	lookupFlags := sc.CommonLookupFlags(requireTable)
	var foundTn *tree.TableName
	foundTn, desc, err = findTableContainingIndex(ctx, txn, sc, tn.Catalog(), tn.Schema(), index.Index, lookupFlags)
	if err != nil {
		return nil, nil, err
	}

	if foundTn != nil {
		// Memoize the table name that was found. tn is a reference to the table name
		// stored in index.Table.
		*tn = *foundTn
	}
	return tn, desc, nil
}

func expandConstraintName(
	ctx context.Context,
	txn *client.Txn,
	sc SchemaResolver,
	constraint *tree.Name,
	table *tree.TableName,
	requireTable bool,
) (tn *tree.TableName, desc *MutableTableDescriptor, err error) {
	tn = table
	if tn.Table() != "" {
		// The constraint and its table prefix must exist already. Resolve the table.
		descTemp, err := ResolveMutableExistingObject(ctx, sc, tn, requireTable, requireTableDesc)
		if err != nil {
			return nil, nil, err
		}
		desc, ok := descTemp.(*MutableTableDescriptor)
		if !ok {
			return nil, nil, sqlbase.NewUndefinedRelationError(tn)
		}
		return tn, desc, nil
	}

	// On the first call to expandMutableConstraintName(), constraint.Table.Table() is empty.
	// Once the table name is resolved for the constraint below, constraint.Table
	// references the table name.

	// Look up the table prefix.
	found, _, err := tn.TableNamePrefix.Resolve(ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, nil, err
	}
	if !found {
		if requireTable {
			return nil, nil, pgerror.NewErrorf(pgcode.UndefinedObject,
				"schema or database was not found while searching constraint: %q",
				tree.ErrString(constraint)).SetHintf(
				"check the current database and search_path are valid")
		}
		return nil, nil, nil
	}

	lookupFlags := sc.CommonLookupFlags(requireTable)
	var foundTn *tree.TableName
	foundTn, desc, err = findTableContainingConstraint(ctx, txn, sc, tn.Catalog(), tn.Schema(), tree.UnrestrictedName(constraint.String()), lookupFlags)
	if err != nil {
		return nil, nil, err
	}

	if foundTn != nil {
		// Memoize the table name that was found. tn is a reference to the table name
		// stored in constraint.Table.
		*tn = *foundTn
	}
	return tn, desc, nil
}

// getTableAndIndex returns the table and index descriptors for a table
// (primary index) or table-with-index. Only one of table and tableWithIndex can
// be set.  This is useful for statements that have both table and index
// variants (like `ALTER TABLE/INDEX ... SPLIT AT ...`).
// It can return indexes that are being rolled out.
func (p *planner) getTableAndIndex(
	ctx context.Context,
	table *tree.TableName,
	tableWithIndex *tree.TableIndexName,
	privilege privilege.Kind,
) (*MutableTableDescriptor, *sqlbase.IndexDescriptor, error) {
	var tableDesc *MutableTableDescriptor
	var err error
	if tableWithIndex == nil {
		// Variant: ALTER TABLE
		tableDesc, err = p.ResolveMutableTableDescriptor(
			ctx, table, true /*required*/, requireTableDesc,
		)
	} else {
		// Variant: ALTER INDEX
		_, tableDesc, err = expandMutableIndexName(ctx, p, tableWithIndex, true /* requireTable */)
	}
	if err != nil {
		return nil, nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege); err != nil {
		return nil, nil, err
	}

	// Determine which index to use.
	var index *sqlbase.IndexDescriptor
	if tableWithIndex == nil {
		index = &tableDesc.PrimaryIndex
	} else {
		idx, dropped, err := tableDesc.FindIndexByName(string(tableWithIndex.Index))
		if err != nil {
			return nil, nil, err
		}
		if dropped {
			return nil, nil, fmt.Errorf("index %q being dropped", tableWithIndex.Index)
		}
		index = idx
	}
	return tableDesc, index, nil
}

// getTableAndConstraint returns the table and constraint descriptors for a table
// (primary index) or table-with-constraint. Only one of table and tableWithConstraint can
// be set.  This is useful for statements that have both table and constraint
// variants (like `ALTER TABLE/CONSTRAINT ... SPLIT AT ...`).
// It can return constraints that are being rolled out.
func (p *planner) getTableAndConstraint(
	ctx context.Context,
	table *tree.TableName,
	tableWithConstraint *tree.Name,
	privilege privilege.Kind,
) (*MutableTableDescriptor, *sqlbase.ConstraintDetail, error) {
	var tableDesc *MutableTableDescriptor
	var err error
	if tableWithConstraint == nil {
		// Variant: ALTER TABLE
		tableDesc, err = p.ResolveMutableTableDescriptor(
			ctx, table, true /*required*/, requireTableDesc,
		)
	} else {
		// Variant: ALTER CONSTRAINT
		_, tableDesc, err = expandMutableConstraintName(ctx, p, tableWithConstraint, table, true /* requireTable */)
	}
	if err != nil {
		return nil, nil, err
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege); err != nil {
		return nil, nil, err
	}

	// Determine which constraint to use.
	var constraint *sqlbase.ConstraintDetail
	if tableWithConstraint == nil {
		constraint = nil
	} else {
		cst, dropped, err := tableDesc.FindConstraintByName(ctx, p.Txn(), string(*tableWithConstraint))
		if err != nil {
			return nil, nil, err
		}
		if dropped {
			return nil, nil, fmt.Errorf("constraint %q being dropped", tableWithConstraint)
		}
		constraint = cst
	}
	return tableDesc, constraint, nil
}

// expandTableGlob expands pattern into a list of tables represented
// as a tree.TableNames.
func expandTableGlob(
	ctx context.Context, txn *client.Txn, sc SchemaResolver, pattern tree.TablePattern,
) (tree.TableNames, error) {
	if t, ok := pattern.(*tree.TableName); ok {
		_, err := ResolveExistingObject(ctx, sc, t, true /*required*/, anyDescType)
		if err != nil {
			return nil, err
		}
		return tree.TableNames{*t}, nil
	}

	glob := pattern.(*tree.AllTablesSelector)
	found, descI, err := glob.TableNamePrefix.Resolve(
		ctx, sc, sc.CurrentDatabase(), sc.CurrentSearchPath())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sqlbase.NewInvalidWildcardError(tree.ErrString(glob))
	}

	return GetObjectNames(ctx, txn, sc, descI.(*DatabaseDescriptor), []string{glob.Schema()}, glob.ExplicitSchema)
}

// fkSelfResolver is a SchemaResolver that inserts itself between a
// user of name resolution and another SchemaResolver, and will answer
// lookups of the new table being created. This is needed in the case
// of CREATE TABLE with a foreign key self-reference: the target of
// the FK definition is a table that does not exist yet.
type fkSelfResolver struct {
	SchemaResolver
	newTableName *tree.TableName
	newTableDesc *sqlbase.TableDescriptor
}

var _ SchemaResolver = &fkSelfResolver{}

// LookupObject implements the tree.TableNameExistingResolver interface.
func (r *fkSelfResolver) LookupObject(
	ctx context.Context, requireMutable bool, requireFunction bool, dbName, scName, tbName string,
) (found bool, objMeta tree.NameResolutionResult, err error) {
	if dbName == r.newTableName.Catalog() &&
		scName == r.newTableName.Schema() &&
		tbName == r.newTableName.Table() {
		table := r.newTableDesc
		if requireMutable {
			return true, sqlbase.NewMutableExistingTableDescriptor(*table), nil
		}
		return true, sqlbase.NewImmutableTableDescriptor(*table), nil
	}
	return r.SchemaResolver.LookupObject(ctx, requireMutable, requireFunction, dbName, scName, tbName)
}

// internalLookupCtx can be used in contexts where all descriptors
// have been recently read, to accelerate the lookup of
// inter-descriptor relationships.
//
// This is used mainly in the generators for virtual tables,
// aliased as tableLookupFn below.
//
// It only reveals physical descriptors (not virtual descriptors).
type internalLookupCtx struct {
	dbNames map[sqlbase.ID]string
	dbIDs   []sqlbase.ID
	dbDescs map[sqlbase.ID]*DatabaseDescriptor
	scDescs map[sqlbase.ID]*SchemaDescriptor
	scIDs   []sqlbase.ID
	tbDescs map[sqlbase.ID]*TableDescriptor
	tbIDs   []sqlbase.ID

	funcDescs map[sqlbase.ID]*FunctionDescriptor
	funcIDs   []sqlbase.ID
}

// tableLookupFn can be used to retrieve a table descriptor and its corresponding
// database descriptor using the table's ID.
type tableLookupFn = *internalLookupCtx

func newInternalLookupCtx(
	descs []sqlbase.DescriptorProto, prefix *DatabaseDescriptor,
) *internalLookupCtx {
	dbNames := make(map[sqlbase.ID]string)
	dbDescs := make(map[sqlbase.ID]*DatabaseDescriptor)
	scDescs := make(map[sqlbase.ID]*SchemaDescriptor)
	tbDescs := make(map[sqlbase.ID]*TableDescriptor)
	funcDescs := make(map[sqlbase.ID]*FunctionDescriptor)
	var tbIDs, dbIDs, scIDs, funcIDs []sqlbase.ID
	// Record database descriptors for name lookups.
	for _, desc := range descs {
		switch d := desc.(type) {
		case *sqlbase.DatabaseDescriptor:
			dbNames[d.ID] = d.Name
			dbDescs[d.ID] = d
			if prefix == nil || prefix.ID == d.ID {
				dbIDs = append(dbIDs, d.ID)
			}
		case *sqlbase.SchemaDescriptor:
			scDescs[d.ID] = d
			if prefix == nil || prefix.ID == d.ParentID {
				scIDs = append(scIDs, d.ID)
			}
		case *sqlbase.TableDescriptor:
			tbDescs[d.ID] = d
		case *sqlbase.FunctionDescriptor:
			funcDescs[d.ID] = d
		}
	}

	for _, d := range tbDescs {
		if prefix == nil || prefix.ID == d.ParentID {
			// Only make the table visible for iteration if the prefix was included.
			tbIDs = append(tbIDs, d.ID)
		} else {
			if scDesc, ok := scDescs[d.ParentID]; ok {
				if prefix.ID == scDesc.ParentID {
					tbIDs = append(tbIDs, d.ID)
				}
			}
		}
	}
	for _, d := range funcDescs {
		if prefix == nil || prefix.ID == d.ParentID {
			// Only make the function visible for iteration if the prefix was included.
			funcIDs = append(funcIDs, d.ID)
		} else {
			if scDesc, ok := scDescs[d.ParentID]; ok {
				if prefix.ID == scDesc.ParentID {
					funcIDs = append(funcIDs, d.ID)
				}
			}
		}
	}

	return &internalLookupCtx{
		dbNames: dbNames,
		dbDescs: dbDescs,
		scDescs: scDescs,
		tbDescs: tbDescs,
		dbIDs:   dbIDs,
		scIDs:   scIDs,
		tbIDs:   tbIDs,

		funcDescs: funcDescs,
		funcIDs:   funcIDs,
	}
}

func (l *internalLookupCtx) getDatabaseByID(id sqlbase.ID) (*DatabaseDescriptor, error) {
	db, ok := l.dbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

func (l *internalLookupCtx) getSchemaByID(id sqlbase.ID) (*SchemaDescriptor, error) {
	sc, ok := l.scDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

func (l *internalLookupCtx) getTableByID(id sqlbase.ID) (*TableDescriptor, error) {
	tb, ok := l.tbDescs[id]
	if !ok {
		return nil, sqlbase.NewUndefinedRelationError(
			tree.NewUnqualifiedTableName(tree.Name(fmt.Sprintf("[%d]", id))))
	}
	return tb, nil
}

func (l *internalLookupCtx) getParentName(table *TableDescriptor) (string, string) {
	dbName := l.dbNames[table.GetParentID()]
	if dbName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this virtual table before the dropped table descriptors are
		// effectively deleted.
		sc, err := l.getSchemaByID(table.GetParentID())
		if sc == nil || err != nil {
			return "", fmt.Sprintf("[%d]", table.GetParentID())
		}
		dbName = l.dbNames[sc.ParentID]
		if dbName == "" {
			dbName = fmt.Sprintf("[%d]", sc.ParentID)
		}
		return dbName, sc.Name
	}
	return dbName, tree.PublicSchema
}

func (l *internalLookupCtx) getFuncParentName(function *FunctionDescriptor) (string, string) {
	dbName := l.dbNames[function.GetParentID()]
	if dbName == "" {
		// The parent database was deleted. This is possible e.g. when
		// a database is dropped with CASCADE, and someone queries
		// this virtual function before the dropped function descriptors are
		// effectively deleted.
		sc, err := l.getSchemaByID(function.GetParentID())
		if sc == nil || err != nil {
			return "", fmt.Sprintf("[%d]", function.GetParentID())
		}
		dbName = l.dbNames[sc.ParentID]
		if dbName == "" {
			dbName = fmt.Sprintf("[%d]", sc.ParentID)
		}
		return dbName, sc.Name
	}
	return dbName, tree.PublicSchema
}

func checkRelationType(
	ctx context.Context,
	p *planner,
	relationNum int,
	relationTarget tree.TablePattern,
	relationsType requiredType,
) ([]sqlbase.DescriptorProto, error) {
	descs := make([]sqlbase.DescriptorProto, 0, relationNum)
	relationGlob, err := relationTarget.NormalizeTablePattern()
	if err != nil {
		return nil, err
	}

	_, ok := relationGlob.(*tree.AllTablesSelector)

	relationNames, err := expandTableGlob(ctx, p.txn, p, relationGlob)
	if err != nil {
		return nil, err
	}

	for i := range relationNames {
		descriptorTemp, err := ResolveMutableExistingObject(ctx, p, &relationNames[i], true, anyDescType)
		if err != nil {
			return nil, err
		}
		descriptor, ok2 := descriptorTemp.(*MutableTableDescriptor)
		if !ok2 {
			return nil, sqlbase.NewUndefinedRelationError(&relationNames[i])
		}
		rType := relationsType
		if CheckVirtualSchema(relationNames[i].Schema()) {
			if rType != requireViewDesc {
				return nil, sqlbase.NewWrongObjectTypeError(&relationNames[i], requiredTypeNames[rType])
			}
			rType = requireTableDesc
		}
		switch rType {
		case requireTableDesc:
			if !descriptor.IsTable() {
				if ok {
					continue
				}
				return nil, sqlbase.NewWrongObjectTypeError(&relationNames[i], requiredTypeNames[requireTableDesc])
			}
		case requireViewDesc:
			if !descriptor.IsView() {
				if ok {
					continue
				}
				return nil, sqlbase.NewWrongObjectTypeError(&relationNames[i], requiredTypeNames[requireViewDesc])
			}
		case requireSequenceDesc:
			if !descriptor.IsSequence() {
				if ok {
					continue
				}
				return nil, sqlbase.NewWrongObjectTypeError(&relationNames[i], requiredTypeNames[requireSequenceDesc])
			}
		}
		descs = append(descs, descriptor)
	}
	return descs, nil
}

func checkFunctionExist(ctx context.Context, p *planner, n tree.Function) error {
	var database = ""
	var schemaName = ""

	if n.FuncName.ExplicitCatalog {
		database = n.FuncName.Catalog()
	} else {
		database = p.CurrentDatabase()
	}
	if n.FuncName.ExplicitSchema {
		schemaName = n.FuncName.Schema()
	} else {
		schemaName = p.CurrentSearchPath().String()
	}
	databaseID, err := getDatabaseID(ctx, p.Txn(), database, true)
	if err != nil {
		return err
	}
	schemaID, err := getSchemaID(ctx, p.Txn(), databaseID, schemaName, true)
	if err != nil {
		return err
	}

	paraStr, err := n.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return err
	}

	keyName := n.FuncName.Table() + paraStr
	key := functionKey{parentID: schemaID, name: keyName, funcName: n.FuncName.Table()}
	exist, err := descExists(ctx, p.txn, key.Key())
	if err != nil {
		return err
	}

	if !exist {
		return sqlbase.NewUndefinedFunctionError(n.FuncName.Table())
	}

	return nil
}

func getSchemaIDByFunction(
	ctx context.Context, p *planner, function tree.Function,
) (sqlbase.ID, error) {
	var database = ""
	var schemaName = ""

	if function.FuncName.ExplicitCatalog {
		database = function.FuncName.Catalog()
	} else {
		database = p.CurrentDatabase()
	}
	if function.FuncName.ExplicitSchema {
		schemaName = function.FuncName.Schema()
	} else {
		schemaName = p.CurrentSearchPath().String()
	}

	databaseID, err := getDatabaseID(ctx, p.Txn(), database, true)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	schemaID, err := getSchemaID(ctx, p.Txn(), databaseID, schemaName, true)
	if err != nil {
		return sqlbase.InvalidID, err
	}
	return schemaID, nil
}
