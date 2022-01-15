package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type renameFunctionNode struct {
	n        *tree.RenameFunction
	funcDesc *sqlbase.FunctionDescriptor
}

// RenameFunction rename a procedure
func (p *planner) RenameFunction(ctx context.Context, n *tree.RenameFunction) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support user define function now!")
	}
	// todo(xz): may get funcDesc by using p.ResolveMutableTableDescriptor
	if n.FuncName.Table() == "" || n.NewName.Table() == "" {
		return nil, errEmptyFunctionName
	}

	// get old && new database descriptor, this step can fill funcName
	// and check if newFuncName has an ExplicitCatalog and an ExplicitSchema
	oldDbDesc, err := getDbDescByTable(ctx, p, &n.FuncName)

	if err != nil {
		return nil, err
	}
	_, err = getDbDescByTable(ctx, p, &n.NewName)
	if err != nil {
		return nil, err
	}

	if isInternal := CheckVirtualSchema(n.NewName.Schema()); isInternal {
		errorFuncName := "function"
		if !n.IsFunction {
			errorFuncName = "procedure"
		}
		return nil, fmt.Errorf("cannot create %s in virtual schema: %q", errorFuncName, n.NewName.Schema())
	}

	// need compare two old and new funcName,
	// if oldName has ExplicitCatalog, but newName not, set newName's catalogName to old catalogName
	// if oldName has ExplicitSchema, but newName not, set newName's schemaName to old schemaName
	if n.FuncName.ExplicitCatalog && !n.NewName.ExplicitCatalog {
		n.NewName.CatalogName = n.FuncName.CatalogName
		n.NewName.ExplicitCatalog = true
	}
	if n.FuncName.ExplicitSchema && !n.NewName.ExplicitSchema {
		n.NewName.SchemaName = n.FuncName.SchemaName
		n.NewName.ExplicitSchema = true
	}
	if n.FuncName.Catalog() != n.NewName.Catalog() || n.FuncName.Schema() != n.NewName.Schema() {
		n.HasNewSchema = true
	}
	if n.FuncName.Equals(&n.NewName) {
		return nil, errors.Errorf("new procedure name is the same as old procedure name, rename canceled.\n")
	}
	// get new schema descriptor
	newScDesc, err := getScDesc(ctx, p, n.NewName.Schema(), &n.NewName)
	if err != nil {
		return nil, err
	}

	// get function descriptor
	paraStr, err := n.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return nil, err
	}
	keyName := n.FuncName.Table() + paraStr
	newkeyName := n.NewName.Table() + paraStr
	funcDesc, err := getFuncDesc(ctx, p, n.FuncName.Schema(), oldDbDesc, keyName)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, funcDesc, privilege.DROP); err != nil {
		return nil, err
	}
	if err := p.CheckPrivilege(ctx, newScDesc, privilege.CREATE); err != nil {
		return nil, err
	}
	newKey := functionKey{parentID: newScDesc.GetID(), name: newkeyName, funcName: n.NewName.Table()}
	exist, err := descExists(ctx, p.txn, newKey.Key())
	if exist {
		return nil, errors.Errorf("new procedure name existed,please change the new name and retry.\n")
	}
	// 当新名称指定了schema并且和old schema不同时，parentID才为 newScDesc.ID
	funcDesc.ParentID = newScDesc.ID
	funcDesc.Name = n.NewName.Table()
	leftBracketsIdx := strings.Index(funcDesc.FullFuncName, "(")
	if leftBracketsIdx > -1 {
		newFuncFullName := funcDesc.Name + funcDesc.FullFuncName[leftBracketsIdx:]
		funcDesc.FullFuncName = newFuncFullName
	} else {
		return nil, sqlbase.NewUndefinedFunctionError(funcDesc.Name)
	}

	renameDetails := sqlbase.FunctionDescriptor_NameInfo{
		ParentID: oldDbDesc.GetSchemaID(n.FuncName.Schema()),
		Name:     keyName}
	funcDesc.DrainingNames = append(funcDesc.DrainingNames, renameDetails)

	return &renameFunctionNode{n: n, funcDesc: funcDesc}, nil
}

func (n *renameFunctionNode) startExec(params runParams) error {
	var PoF string
	if n.n.IsFunction {
		PoF = string(EventLogRenameFunction)
	} else {
		PoF = string(EventLogRenameProcedure)
	}
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: PoF,
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.funcDesc.ID),
			Desc: struct {
				name string
			}{
				name: n.funcDesc.Name,
			},
		},
		Info: "SHOW SYNTAX: " + params.p.stmt.SQL,
	}
	return renameFuncDesc(n, params)
}

func (n *renameFunctionNode) Next(params runParams) (bool, error) {
	return false, nil
}

func (*renameFunctionNode) Values() tree.Datums   { return tree.Datums{} }
func (*renameFunctionNode) Close(context.Context) {}

// renameFuncDesc rename function descriptor when rename function descriptor
func renameFuncDesc(n *renameFunctionNode, params runParams) error {
	leaseMgr := params.p.extendedEvalCtx.Tables.leaseMgr
	oldScDesc, err := getScDesc(params.ctx, params.p, n.n.FuncName.Schema(), &n.n.FuncName)
	if err == nil && oldScDesc != nil {
		cacheKey := makeTableNameCacheKey(oldScDesc.ID, n.n.FuncName.TableName.String())
		if _, ok := leaseMgr.functionNames.functions[cacheKey]; ok {
			leaseMgr.functionNames.removeFuncGrpByIDAndName(oldScDesc.ID, n.n.FuncName.TableName.String())
		}
	}
	newScDesc, err := getScDesc(params.ctx, params.p, n.n.NewName.Schema(), &n.n.NewName)
	if err == nil && newScDesc != nil {
		cacheKey := makeTableNameCacheKey(newScDesc.ID, n.n.NewName.TableName.String())
		if _, ok := leaseMgr.functionNames.functions[cacheKey]; ok {
			leaseMgr.functionNames.removeFuncGrpByIDAndName(newScDesc.ID, n.n.NewName.TableName.String())
		}
	}
	p := params.p
	ctx := params.ctx
	mutableFuncDesc := sqlbase.NewMutableExistingFunctionDescriptor(*n.funcDesc)
	if err := p.writeFunctionSchemaChange(params.ctx, mutableFuncDesc, sqlbase.InvalidMutationID); err != nil {
		return err
	}

	// get funcName key
	paraStr, err := n.n.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return err
	}
	newName := n.funcDesc.Name + paraStr
	newFuncKey := functionKey{parentID: n.funcDesc.ParentID, name: newName, funcName: n.funcDesc.Name}
	newFuncIDKey := newFuncKey.Key()

	// rename operator not change the id of the function descriptor
	funcDescID := n.funcDesc.ID

	funcDesc := sqlbase.WrapDescriptor(n.funcDesc)
	funcDescKey := sqlbase.MakeDescMetadataKey(0)
	// We update the descriptor to the new name, but also leave the mapping of the
	// old name to the id, so that the name is not reused until the schema changer
	// has made sure it's not in use any more.
	b := &client.Batch{}
	if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "Put %s -> %s", funcDescKey, funcDesc)
		log.VEventf(ctx, 2, "CPut %s -> %d", newFuncIDKey, funcDescID)
	}
	b.Put(funcDescKey, funcDesc)
	b.CPut(newFuncIDKey, funcDescID, nil)

	if err := mutableFuncDesc.ValidateFunction(params.EvalContext().Settings); err != nil {
		return err
	}
	if err := p.Tables().addUncommittedFunction(*mutableFuncDesc); err != nil {
		return err
	}

	return p.txn.Run(ctx, b)
}
