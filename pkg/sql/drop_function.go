package sql

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type dropFunctionNode struct {
	n  *tree.DropFunction
	td *toDeleteFunction
	// td []toDeleteFunction
}

type toDeleteFunction struct {
	tn   *tree.TableName
	desc *sqlbase.MutableFunctionDescriptor
}

func (p *planner) DropFunction(ctx context.Context, n *tree.DropFunction) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support user define function now!")
	}
	if n.FuncName.String() == "" {
		return nil, errEmptyFunctionName
	}

	// from name resolve database and schema descriptor
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.FuncName)
	if err != nil {
		return nil, err
	}
	paraStr, err := n.Parameters.GetFuncParasTypeStr()
	if err != nil {
		return nil, err
	}
	keyName := n.FuncName.Table() + paraStr

	// try to get function descriptor through function name and arg types
	funcDesc, err := getFuncDesc(ctx, p, n.FuncName.Schema(), dbDesc, keyName)
	toDeleteFunc := &toDeleteFunction{}
	if err != nil {
		if n.MissingOk {
			toDeleteFunc = nil
		} else {
			if pgErr, ok := err.(*pgerror.Error); ok && pgErr.Code == pgcode.UndefinedFunction {
				errMsgFmt := "%s %s does not exist"
				funcTypeStr := "function"
				if !n.IsFunction {
					funcTypeStr = "procedure"
				}
				pgErr.Message = fmt.Sprintf(errMsgFmt, funcTypeStr, keyName)
				return nil, pgErr
			}
			return nil, err
		}
	} else {
		// ensure use DROP PROCEDURE to drop a procedure, use DROP FUNCTION to drop a function
		if funcDesc.IsProcedure && n.IsFunction {
			return nil, errors.Errorf("%s is not a function, to drop a procedure, use DROP PROCEDURE ...", keyName)
		} else if !funcDesc.IsProcedure && !n.IsFunction {
			return nil, errors.Errorf("%s is not a procedure, to drop a function, use DROP FUNCTION ...", keyName)
		}
		if err := p.CheckPrivilege(ctx, funcDesc, privilege.DROP); err != nil {
			return nil, err
		}
		toDeleteFunc = &toDeleteFunction{tn: &n.FuncName, desc: sqlbase.NewMutableCreatedFunctionDescriptor(*funcDesc)}
		// remove function group function lease manager.functionNames
		p.extendedEvalCtx.Tables.leaseMgr.functionNames.removeFuncGrpByIDAndName(funcDesc.ParentID, funcDesc.Name)
	}

	return &dropFunctionNode{n: n, td: toDeleteFunc}, nil
}

func (n *dropFunctionNode) startExec(params runParams) error {
	if n.n.MissingOk && n.td == nil {
		return nil
	}
	var PoF string
	if n.n.IsFunction {
		PoF = string(EventLogDropFunction)
	} else {
		PoF = string(EventLogDropProcedure)
	}
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: PoF,
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.td.desc.ID),
			Desc: struct {
				name string
			}{
				name: n.td.desc.Name,
			},
		},
		Info: "SHOW SYNTAX: " + params.p.stmt.SQL,
	}
	return dropOldFunctionKeys(params.ctx, params.p.txn, n.n.FuncName, n.n.Parameters, n.td.desc)
}

func (*dropFunctionNode) Next(runParams) (bool, error) { return false, nil }
func (*dropFunctionNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropFunctionNode) Close(context.Context)        {}

// prepareDropFunc/dropFuncImpl is used to drop a single function by
// name, which can result from a DROP FUNCTION statement. This method
// returns the dropped function descriptor, to be used for the purpose
// of logging the event.  The function is not actually truncated or
// deleted synchronously. Instead, it is marked as deleted (meaning
// up_version is set and deleted is set) and the actual deletion happens
// async in a schema changer. Note that, courtesy of up_version, the
// actual truncation and dropping will only happen once every node ACKs
// the version of the descriptor with the deleted bit set, meaning the
// lease manager will not hand out new leases for it and existing leases
// are released).
// If the function does not exist, this function returns a nil descriptor.
func (p *planner) prepareDropFunc(
	ctx context.Context, name *tree.TableName, required bool, requiredType requiredType,
) ([]sqlbase.MutableFunctionDescriptor, error) {
	funcGrp, err := p.ResolveMutableFuncDescriptorGrp(ctx, name, required, requiredType)
	if err != nil {
		return nil, err
	}
	if funcGrp == nil {
		//return nil, err
		return nil, errors.New("")
	}
	for _, mFuncDesc := range funcGrp.FuncMutGroup {
		if err := p.CheckPrivilege(ctx, &mFuncDesc, privilege.DROP); err != nil {
			return nil, err
		}
	}

	return funcGrp.FuncMutGroup, nil
}
