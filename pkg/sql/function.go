package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/lib/pq/oid"
	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// functionKey implements sqlbase.DescriptorKey.
type functionKey namespaceKey

func (fk functionKey) Key() roachpb.Key {
	return sqlbase.MakeFuncNameMetadataKey(fk.parentID, fk.name, fk.funcName)
}

func (fk functionKey) Name() string {
	return fk.name
}

// createFunction takes Function descriptor and creates it if needed,
// incrementing the descriptor counter. Returns true if the descriptor
// is actually created, false if it already existed, or an error if one was
// encountered. The ifNotExists flag is used to declare if the "already existed"
// state should be an error (false) or a no-op (true).
// createFunction implements the FunctionDescEditor interface.
// Similar to p.createDatabase in create_database.go, p.createSchema in crate_schema.go
// todo(xz): there is no IF NOT EXIST when create function, so ifNotExists default to false
func (p *planner) createFunction(
	ctx context.Context, desc *sqlbase.FunctionDescriptor, node *createFunctionNode, ifNotExists bool,
) (bool, error) {

	plainKey := functionKey{
		parentID: desc.GetParentID(),
		name:     node.fullFuncName,
		funcName: node.n.FuncName.Table(),
	}
	idKey := plainKey.Key()
	desc.FullFuncName = node.fullFuncName

	// verify function exists or not, create descriptor with id
	if exists, err := descExists(ctx, p.txn, idKey); err == nil && exists {
		if ifNotExists {
			// Noop.
			return false, nil
		}
		return false, sqlbase.NewFunctionAlreadyExistsError(desc.Name)
	} else if err != nil {
		return false, err
	}

	id, err := GenerateUniqueDescID(ctx, p.ExecCfg().DB)
	if err != nil {
		return false, err
	}
	// set id to descriptor and write descriptor to store
	return true, p.createDescriptorWithID(ctx, idKey, id, desc, nil)
}

// writeFuncSchemaChange effectively writes a function descriptor to the
// database within the current Planner transaction, and queues up
// a schema changer for future processing.
func (p *planner) writeFunctionSchemaChange(
	ctx context.Context, funcDesc *sqlbase.MutableFunctionDescriptor, mutationID sqlbase.MutationID,
) error {
	if funcDesc.Dropped() {
		// We don't allow schema changes on a dropped function.
		return fmt.Errorf("function %q is being dropped", funcDesc.Name)
	}
	return p.writeFunctionDesc(ctx, funcDesc, mutationID)
}

func (p *planner) writeFunctionSchemaChangeToBatch(
	ctx context.Context,
	funcDesc *sqlbase.MutableFunctionDescriptor,
	mutationID sqlbase.MutationID,
	b *client.Batch,
) error {
	if funcDesc.Dropped() {
		// We don't allow schema changes on a dropped function.
		return fmt.Errorf("function %q is being dropped", funcDesc.Name)
	}
	return p.writeFunctionDescToBatch(ctx, funcDesc, mutationID, b)
}

//
// func (p *planner) writeDropFunction(
// 	ctx context.Context, funcDesc *sqlbase.MutableFunctionDescriptor,
// ) error {
// 	return p.writeFunctionDesc(ctx, funcDesc, sqlbase.InvalidMutationID)
// }

func (p *planner) writeFunctionDesc(
	ctx context.Context, funcDesc *sqlbase.MutableFunctionDescriptor, mutationID sqlbase.MutationID,
) error {
	b := p.txn.NewBatch()
	if err := p.writeFunctionDescToBatch(ctx, funcDesc, mutationID, b); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeFunctionDescToBatch(
	ctx context.Context,
	funcDesc *sqlbase.MutableFunctionDescriptor,
	mutationID sqlbase.MutationID,
	b *client.Batch,
) error {

	if funcDesc.IsNewFunction() {
		if err := runFunctionSchemaChangesInTxn(ctx,
			p.txn,
			nil,
			p.execCfg,
			p.EvalContext(),
			funcDesc,
			p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
		); err != nil {
			return err
		}
	} else {
		// Only increment the function descriptor version once in this transaction.
		if err := maybeIncrementFunctionVersion(ctx, funcDesc, p.txn); err != nil {
			return err
		}

		// Schedule a function changer for later.
		p.queueFunctionSchemaChange(funcDesc.FunctionDesc(), mutationID)
	}

	if err := funcDesc.Validate(); err != nil {
		return pgerror.NewAssertionErrorf("function descriptor is not valid: %s\n%v", err, funcDesc)
	}

	if err := p.Tables().addUncommittedFunction(*funcDesc); err != nil {
		return err
	}

	descKey := sqlbase.MakeDescMetadataKey(funcDesc.GetID())
	descVal := sqlbase.WrapDescriptor(funcDesc)
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Put %s -> %s", descKey, descVal)
	}

	b.Put(descKey, descVal)
	return nil
}

// queueSchemaChange queues up a schema changer to process an outstanding
// schema change for the function.
func (p *planner) queueFunctionSchemaChange(
	funcDesc *sqlbase.FunctionDescriptor, mutationID sqlbase.MutationID,
) {
	sc := SchemaChanger{
		tableID:              funcDesc.GetID(),
		mutationID:           mutationID,
		nodeID:               p.extendedEvalCtx.NodeID,
		leaseMgr:             p.LeaseMgr(),
		jobRegistry:          p.ExecCfg().JobRegistry,
		leaseHolderCache:     p.ExecCfg().LeaseHolderCache,
		rangeDescriptorCache: p.ExecCfg().RangeDescriptorCache,
		clock:                p.ExecCfg().Clock,
		settings:             p.ExecCfg().Settings,
		execCfg:              p.ExecCfg(),
		isFunc:               true,
	}
	p.extendedEvalCtx.SchemaChangers.queueSchemaChanger(sc)
}

// getFunctionID resolves a schema name and schema parentID into a schema ID.
// Returns InvalidID on failure.
func getFunctionID(
	ctx context.Context, txn *client.Txn, parentID sqlbase.ID, name string, required bool,
) (sqlbase.ID, error) {
	// when the parentID is InvalidID, return schemaID is InvalidID
	if parentID == sqlbase.InvalidID {
		return sqlbase.InvalidID, nil
	}
	index := strings.Index(name, "(")
	funcName := ""
	if index > -1 {
		funcName = name[:index]
	} else {
		return sqlbase.InvalidID, sqlbase.NewUndefinedFunctionError(name)
	}
	fcID, err := getFunctionDescriptorID(ctx, txn, functionKey{parentID: parentID, name: name, funcName: funcName})
	if err != nil {
		return sqlbase.InvalidID, err
	}
	if fcID == sqlbase.InvalidID && required {
		return fcID, sqlbase.NewUndefinedFunctionError(name)
	}
	return fcID, nil
}

// GetAllFuncDescs get all function descriptors
func GetAllFuncDescs(ctx context.Context, txn *client.Txn) ([]*FunctionDescriptor, error) {
	allDesc, err := GetAllDescriptors(ctx, txn)
	if err != nil {
		return nil, err
	}
	allFuncDescs := make([]*FunctionDescriptor, 0)
	for _, desc := range allDesc {
		if fDesc, ok := desc.(*FunctionDescriptor); ok && !fDesc.Dropped() {
			allFuncDescs = append(allFuncDescs, fDesc)
		}
	}
	return allFuncDescs, nil
}

func getFuncDesc(
	ctx context.Context,
	p *planner,
	schema string,
	dbDesc *UncachedDatabaseDescriptor,
	keyName string,
) (*FunctionDescriptor, error) {
	funcID, err := getFunctionID(ctx, p.txn, dbDesc.GetSchemaID(schema), keyName, true)
	if err != nil {
		return nil, err
	}
	mutableFuncDesc, err := sqlbase.GetMutableFunctionDescFromID(ctx, p.txn, funcID)
	if err != nil {
		return nil, err
	}
	funcDesc := mutableFuncDesc.FunctionDescriptor
	return &funcDesc, nil
}

func getDbDescByTable(
	ctx context.Context, p *planner, function *tree.TableName,
) (*DatabaseDescriptor, error) {
	dbDesc, err := p.ResolveUncachedDatabase(ctx, function)
	if err != nil {
		return nil, err
	}

	if dbDesc.ID == keys.SystemDatabaseID {
		return nil, fmt.Errorf("system database cannot be used: %q", dbDesc.Name)
	}
	return dbDesc, nil
}

func getScDesc(
	ctx context.Context, p *planner, schema string, function *tree.TableName,
) (*SchemaDescriptor, error) {
	dbDesc, err := getDbDescByTable(ctx, p, function)
	if err != nil {
		return nil, err
	}
	if isInternal := CheckVirtualSchema(schema); isInternal {
		return nil, fmt.Errorf("virtual schema cannot be modified: %q", schema)
	}
	scDesc, err := dbDesc.GetSchemaByName(schema)
	if err != nil {
		return nil, err
	}

	return scDesc, nil
}

// dropOldFunctionKeys drop old function key name and old function descriptor meta data key
func dropOldFunctionKeys(
	ctx context.Context,
	txn *client.Txn,
	funcName tree.TableName,
	Parameters tree.FunctionParameters,
	funcDesc *sqlbase.MutableFunctionDescriptor,
) error {
	paraStr, err := Parameters.GetFuncParasTypeStr()
	if err != nil {
		return err
	}
	keyName := funcName.Table() + paraStr
	oldFuncKey := functionKey{parentID: funcDesc.ParentID, name: keyName, funcName: funcName.Table()}
	oldKey := oldFuncKey.Key()
	descKey := sqlbase.MakeDescMetadataKey(funcDesc.ID)
	return txn.Del(ctx, oldKey, descKey)
}

// GetColumnTypAndCheckLimit will check length limit for udr variable datum
func GetColumnTypAndCheckLimit(vars tree.UDRVars, idx int, datum tree.Datum) error {
	var datumTyp types.T
	var ok bool
	var err error
	if vars[idx].ColType == nil {
		datumTyp = types.OidToType[vars[idx].VarOid]
		vars[idx].ColType, err = coltypes.DatumTypeToColumnType(datumTyp)
	}
	ok, datumTyp = checkTypValid(vars[idx].ColType)
	if !ok {
		return nil
	}
	// datumTyp := coltypes.CastTargetToDatumType(vars[idx].ColType)
	columnTyp, err := sqlbase.DatumTypeToColumnType(datumTyp)
	if err != nil {
		return err
	}

	columnTyp, err = sqlbase.PopulateTypeAttrs(columnTyp, vars[idx].ColType)
	if err != nil {
		return err
	}

	_, err = sqlbase.LimitValueWidth(columnTyp, datum, &vars[idx].VarName, true)
	if err != nil {
		return err
	}
	return nil
}

// checkTypValid will check coltypes.T to judge
func checkTypValid(t coltypes.T) (ok bool, datumTyp types.T) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			datumTyp = nil
			return
		}
	}()
	typ := coltypes.CastTargetToDatumType(t)
	if typ == nil {
		return false, typ
	}
	return true, typ
}

// perforCastWithoutNull execute cast for datum except dNull
func perforCastWithoutNull(
	ctx *tree.EvalContext, d tree.Datum, t coltypes.CastTargetType,
) (retDatum tree.Datum, err error) {
	retDatum = d
	if _, ok := retDatum.(tree.DNullExtern); !ok {
		retDatum, err = tree.PerformCast(ctx, d, t)
	}
	return retDatum, err
}

func checkIsQueryOrHasReturning(statement tree.Statement) bool {
	switch n := statement.(type) {
	case *tree.Select:
		return true
	case *tree.Insert:
		return resultsNeeded(n.Returning)
	case *tree.Update:
		return resultsNeeded(n.Returning)
	case *tree.Delete:
		return resultsNeeded(n.Returning)
	default:
		return false
	}
}

// GetInoutParametersFromDesc get in out parameters from function descriptor
func GetInoutParametersFromDesc(
	params []sqlbase.FuncArgDescriptor,
) ([]tree.FuncArgNameType, []tree.FuncArgNameType, error) {
	// ins := make([]sqlbase.FuncArgDescriptor, 0)
	// outs := make([]sqlbase.FuncArgDescriptor, 0)

	ins := make([]tree.FuncArgNameType, 0)
	outs := make([]tree.FuncArgNameType, 0)
	var name string
	var typOid oid.Oid
	for _, param := range params {
		tempParam := param
		name = tempParam.Name
		typOid = param.Oid
		typName := param.ColumnTypeString

		switch tempParam.InOutMode {
		case "in":
			ins = append(ins, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamIN})
		case "out":
			outs = append(outs, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamOUT})
		case "inout":
			ins = append(ins, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamINOUT})
			outs = append(outs, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamINOUT})
		default:
			// // these two mode not support now
			// case FuncParamVariadic:
			// case FuncParamTable:
			return nil, nil, errors.Errorf("unsupported function parameter mode%q", tempParam.InOutMode)
		}
	}
	return ins, outs, nil
}

// GetInoutParametersFromNode get in out parameters from CreateFunction node
func GetInoutParametersFromNode(
	params tree.FunctionParameters,
) ([]tree.FuncArgNameType, []tree.FuncArgNameType, error) {
	ins := make([]tree.FuncArgNameType, 0)
	outs := make([]tree.FuncArgNameType, 0)
	var name string
	var typOid oid.Oid
	for _, param := range params {
		// avoid empty param
		if param.Typ == nil {
			continue
		}

		tempParam := param
		name = tempParam.Name
		if _, ok := tempParam.Typ.(*coltypes.TString); ok {
			typOid = tree.TypeNameGetOID["TEXT"]
		} else {
			typStr := strings.ToUpper(tempParam.Typ.TypeName())
			typOid = tree.TypeNameGetOID[typStr]
		}
		typName := tempParam.Typ.ColumnType()

		switch tempParam.Mode {
		case tree.FuncParamIN:
			ins = append(ins, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamIN})
		case tree.FuncParamOUT:
			outs = append(outs, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamOUT})
		case tree.FuncParamINOUT:
			ins = append(ins, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamINOUT})
			outs = append(outs, tree.FuncArgNameType{Name: name, Oid: typOid, TypeName: typName, Mode: tree.FuncParamINOUT})
		default:
			// // these two mode not support now
			// case FuncParamVariadic:
			// case FuncParamTable:
			return nil, nil, errors.Errorf("unsupported function parameter mode%q", tempParam.Mode)
		}
	}
	return ins, outs, nil
}
