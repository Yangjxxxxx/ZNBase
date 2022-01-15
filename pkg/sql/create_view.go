// Copyright 2017 The Cockroach Authors.
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
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlutil"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/log"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

// createViewNode represents a CREATE VIEW statement.
type createViewNode struct {
	n       *tree.CreateView
	dbDesc  *sqlbase.DatabaseDescriptor
	columns sqlbase.ResultColumns
	hasStar bool

	// planDeps tracks which tables and views the view being created
	// depends on. This is collected during the construction of
	// the view query's logical plan.
	planDeps planDependencies
}

// CreateView creates a view.
// Privileges: CREATE on schema plus SELECT on all the selected columns.
//   notes: postgres requires CREATE on database plus SELECT on all the
//						selected columns.
//          mysql requires CREATE VIEW plus SELECT on all the selected columns.
func (p *planner) CreateView(ctx context.Context, n *tree.CreateView) (planNode, error) {
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &n.Name)
	if err != nil {
		return nil, err
	}

	if isInternal := CheckVirtualSchema(n.Name.SchemaName.String()); isInternal {
		return nil, fmt.Errorf("cannot create view in virtual schema: %q", n.Name.SchemaName.String())
	}

	scDesc, err := dbDesc.GetSchemaByName(n.Name.SchemaName.String())
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(ctx, scDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	var planDeps planDependencies
	var sourceColumns sqlbase.ResultColumns
	var hasStar bool
	// To avoid races with ongoing schema changes to tables that the view
	// depends on, make sure we use the most recent versions of table
	// descriptors rather than the copies in the lease cache.
	p.runWithOptions(resolveFlags{skipCache: true}, func() {
		planDeps, sourceColumns, hasStar, err = p.analyzeViewQuery(ctx, n.AsSource)
	})
	if err != nil {
		return nil, err
	}

	// Ensure that all the table names pretty-print as fully qualified,
	// so we store that in the view descriptor.
	//
	// The traversal will update the TableNames in-place, so the changes are
	// persisted in n.AsSource. We exploit the fact that semantic analysis above
	// has populated any missing db/schema details in the table names in-place.
	// We use tree.FormatNode merely as a traversal method; its output buffer is
	// discarded immediately after the traversal because it is not needed further.
	{
		f := tree.NewFmtCtx(tree.FmtParsable)
		f.SetReformatTableNames(
			func(_ *tree.FmtCtx, tn *tree.TableName) {
				// Persist the database prefix expansion.
				if tn.SchemaName != "" {
					// All CTE or table aliases have no schema
					// information. Those do not turn into explicit.
					tn.ExplicitSchema = true
					tn.ExplicitCatalog = true
				}
			},
		)
		f.FormatNode(n.AsSource)
		f.Close() // We don't need the string.
	}

	numColNames := len(n.ColumnNames)
	numColumns := len(sourceColumns)
	if numColNames != 0 && numColNames != numColumns {
		return nil, sqlbase.NewSyntaxError(fmt.Sprintf(
			"CREATE VIEW specifies %d column name%s, but data source has %d column%s",
			numColNames, util.Pluralize(int64(numColNames)),
			numColumns, util.Pluralize(int64(numColumns))))
	}

	log.VEventf(ctx, 2, "collected view dependencies:\n%s", planDeps.String())

	return &createViewNode{
		n:        n,
		dbDesc:   dbDesc,
		columns:  sourceColumns,
		planDeps: planDeps,
		hasStar:  hasStar,
	}, nil
}

func (n *createViewNode) startExec(params runParams) error {
	p := params.p
	ctx := params.ctx

	if n.n.AsSource != nil {
		if selectClause, ok := n.n.AsSource.Select.(*tree.SelectClause); ok {
			for _, exprs := range selectClause.Exprs {
				if expr, ok := exprs.Expr.(*tree.UnresolvedName); ok {
					if expr.Parts[expr.NumParts-1] == "row_num" && exprs.As == "ROWNUM" {
						return fmt.Errorf("must name this expression with a column alias")
					}
				}
			}
		}
	}
	//check if schema is valid
	if isInternal := CheckVirtualSchema(n.n.Name.SchemaName.String()); isInternal {
		return fmt.Errorf("schema cannot be modified: %q", n.n.Name.SchemaName.String())
	}
	viewName := n.n.Name.Table()
	tKey := tableKey{parentID: n.dbDesc.GetSchemaID(n.n.Name.Schema()), name: viewName}
	isTemporary := n.n.Temporary
	backRefMutables := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor, len(n.planDeps))
	for id, updated := range n.planDeps {
		backRefMutable := params.p.Tables().getUncommittedTableByID(id).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}

		if n.n.Materialized && (backRefMutable.Temporary || strings.Contains(backRefMutable.ViewQuery, "pg_temp_")) {
			//TODO: 暂不支持临时表创建物化视图
			if backRefMutable.IsView() {
				return pgerror.NewError(pgcode.FeatureNotSupported, "cannot create materialized view using temporary views")
			}
			return pgerror.NewError(pgcode.FeatureNotSupported, "cannot create materialized view using temporary tables")
		}
		if !isTemporary && backRefMutable.Temporary {
			//TODO:向session 输出警告,暂未实现
			// This notice is sent from pg, let's imitate.
			params.p.noticeSender.BufferNotice(
				pgnotice.Newf(`view "%s" will be a temporary view`, viewName),
			)
			n.n.Temporary = true
			isTemporary = true
		}
		backRefMutables[id] = backRefMutable
	}
	log.VEventf(params.ctx, 2, "dependencies for view %s:\n%s", viewName, n.planDeps.String())
	if isTemporary {
		tempSchemaName := params.p.TemporarySchemaName()
		n.n.Name.TableNamePrefix.SchemaName = tree.Name(tempSchemaName)
		schemaID, err := getTemporarySchemaID(params, tempSchemaName, n.dbDesc, n.n.IsReplace)
		if err != nil {
			return err
		}
		tKey = tableKey{parentID: schemaID, name: n.n.Name.Table()}
	}
	key := tKey.Key()
	var privSrc *sqlbase.PrivilegeDescriptor
	if exists, err := descExists(params.ctx, params.p.txn, key); err == nil && exists {
		if n.n.IsReplace {
			droppedDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.n.Name, true, requireViewDesc)
			if err != nil {
				return err
			}
			if droppedDesc == nil {
				return fmt.Errorf("already exists view '%s' could not find", n.n.Name.TableName)
			}
			for _, ref := range droppedDesc.DependedOnBy {
				err := p.getViewDescForReplaceView(ctx, "view", droppedDesc.Name, droppedDesc.ParentID, ref.ID)
				if err != nil {
					return err
				}
			}
			cascadeDroppedViews, srcPriv, err := p.dropViewImpl1(ctx, droppedDesc, tree.DropDefault)
			privSrc = srcPriv
			if err != nil {
				return err
			}
			// Log a Drop View event for this table. This is an auditable log event
			// and is recorded in the same transaction as the table descriptor
			// update.
			params.p.curPlan.auditInfo = &server.AuditInfo{
				EventTime: timeutil.Now(),
				EventType: string(EventLogDropView),
				TargetInfo: &server.TargetInfo{
					TargetID: int32(droppedDesc.ID),
					Desc: struct {
						ViewName            string
						CascadeDroppedViews []string
					}{
						n.n.Name.FQString(),
						cascadeDroppedViews,
					},
				},
				Info: &infos.DropViewInfo{
					ViewName:            n.n.Name.FQString(),
					Statement:           n.n.String(),
					User:                params.SessionData().User,
					CascadeDroppedViews: cascadeDroppedViews,
				},
			}
			// Remove old name -> id map.
			b := &client.Batch{}
			// Use CPut because we want to remove a specific name -> id map.
			if p.extendedEvalCtx.Tracing.KVTracingEnabled() {
				log.VEventf(ctx, 2, "CPut %s -> nil", key)
			}
			b.CPut(key, nil, droppedDesc.ID)
			if err := p.txn.Run(ctx, b); err != nil {
				return err
			}
		} else if n.n.IfNotExists {
			return nil
		} else {
			return sqlbase.NewRelationAlreadyExistsError(tKey.Name())
		}
	} else if err != nil {
		return err
	}

	updatable, baseID, columnsReflect, columnsReflectWithoutRowid := analyzeViewUpdatable(params.p, n.planDeps, n.n.AsSource, viewCreate, backRefMutables, n.hasStar)

	id, err := GenerateUniqueDescID(params.ctx, params.extendedEvalCtx.ExecCfg.DB)
	if err != nil {
		return err
	}

	// Inherit permissions from the schema descriptor.
	privs := sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, p.User())
	desc, err := n.makeViewTableDesc(
		params,
		viewName,
		n.n.ColumnNames,
		tKey.parentID,
		id,
		n.columns,
		updatable,
		baseID,
		privs,
		columnsReflect,
		columnsReflectWithoutRowid,
	)
	if err != nil {
		return err
	}
	if privSrc != nil {
		desc.Privileges = privSrc
	}
	if n.n.Materialized {
		if !p.EvalContext().TxnImplicit {
			return pgerror.Newf(pgcode.InvalidTransactionState, "cannot create materialized view in an explicit transaction")
		}
		// Ensure all nodes are the correct version.
		if !params.ExecCfg().Settings.Version.IsActive(cluster.VersionMaterializedViews) {
			return pgerror.Newf(pgcode.FeatureNotSupported,
				"all nodes are not the correct version to use materialized views")
		}
		// If the view is materialized, set up some more state on the view descriptor.
		// In particular,
		// * mark the descriptor as a materialized view
		// * mark the state as adding and remember the AsOf time to perform
		//   the view query
		// * use AllocateIDs to give the view descriptor a primary key
		desc.IsMaterializedView = true
		desc.State = sqlbase.TableDescriptor_ADD
		desc.CreateAsOfTime = params.p.Txn().ReadTimestamp()
		if err := desc.AllocateIDs(); err != nil {
			return err
		}
	}
	// Collect all the tables/views this view depends on.
	for backrefID := range n.planDeps {
		desc.DependsOn = append(desc.DependsOn, backrefID)
	}

	if err = params.p.createDescriptorWithID(
		params.ctx, key, id, &desc, params.EvalContext().Settings); err != nil {
		return err
	}

	// Persist the back-references in all referenced table descriptors.
	for _, updated := range n.planDeps {
		backrefID := updated.desc.ID
		backRefMutable := params.p.Tables().getUncommittedTableByID(backrefID).MutableTableDescriptor
		if backRefMutable == nil {
			backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
		}
		for _, dep := range updated.deps {
			// The logical plan constructor merely registered the dependencies.
			// It did not populate the "ID" field of TableDescriptor_Reference,
			// because the ID of the newly created view descriptor was not
			// yet known.
			// We need to do it here.
			dep.ID = desc.ID
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}
		if err := params.p.writeSchemaChange(params.ctx, backRefMutable, sqlbase.InvalidMutationID); err != nil {
			return err
		}
	}

	if err := desc.RefreshValidate(params.ctx, params.p.txn, params.EvalContext().Settings); err != nil {
		return err
	}
	// Log Create View event. This is an auditable log event and is
	// recorded in the same transaction as the table descriptor update.
	tn := tree.MakeTableName(tree.Name(n.dbDesc.Name), tree.Name(viewName))
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogCreateView),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(desc.ID),
			Desc: struct {
				ViewName string
			}{
				tn.FQString(),
			},
		},
		Info: &infos.CreateViewInfo{
			ViewName:  tn.FQString(),
			Statement: tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable),
			User:      params.SessionData().User,
		},
	}
	return nil
}

func (*createViewNode) Next(runParams) (bool, error) { return false, nil }
func (*createViewNode) Values() tree.Datums          { return tree.Datums{} }
func (n *createViewNode) Close(ctx context.Context)  {}

// makeViewTableDesc returns the table descriptor for a new view.
//
// It creates the descriptor directly in the PUBLIC state rather than
// the ADDING state because back-references are added to the view's
// dependencies in the same transaction that the view is created and it
// doesn't matter if reads/writes use a cached descriptor that doesn't
// include the back-references.
func (n *createViewNode) makeViewTableDesc(
	params runParams,
	viewName string,
	columnNames tree.NameList,
	parentID sqlbase.ID,
	id sqlbase.ID,
	resultColumns []sqlbase.ResultColumn,
	updatable bool,
	baseID sqlbase.ID,
	privileges *sqlbase.PrivilegeDescriptor,
	columnsReflec []string,
	columnsReflecWithoutrowid []string,
) (sqlbase.MutableTableDescriptor, error) {
	var creationTable hlc.Timestamp
	desc := InitTableDescriptor(id, parentID, viewName, creationTable, privileges, n.n.Temporary)
	desc.ViewQuery = tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable)
	desc.ViewUpdatable = updatable
	desc.ViewDependsOn = uint32(baseID)
	if n.hasStar {
		str := make([]string, len(resultColumns))
		//TODO: 可更新视图二期移除对原视图viewQuery的修改
		if updatable {
			// select *
			if strings.Contains(n.n.AsSource.Select.String(), "*") {
				if selectstmt, ok := n.n.AsSource.Select.(*tree.SelectClause); ok {
					copy(str, columnsReflecWithoutrowid)
					//解决非法字符问题，增加双引号 如 v"iew->"v""iew"
					//仅修改select * 中的列名
					for i := range resultColumns {
						str[i] = sqlutil.AddExtraMark(str[i], sqlutil.DoubleQuotation)
					}
					// 解决类似 select *,l1 +1 from t问题
					if len(selectstmt.Exprs) > 1 {
						offset := 0
						for i := range selectstmt.Exprs {
							if sel, ok := selectstmt.Exprs[i].Expr.(*tree.UnresolvedName); ok {
								if sel.Star {
									continue
								}
							}
							if _, ok := selectstmt.Exprs[i].Expr.(tree.UnqualifiedStar); !ok {
								colName := selectstmt.Exprs[i].Expr.String()
								str[len(columnsReflecWithoutrowid)+offset] = colName
								offset++
							}
						}
					}
				}
			} else {
				//select a from t order by t.*
				for i := range str {
					str[i] = sqlutil.AddExtraMark(resultColumns[i].Name, sqlutil.DoubleQuotation)
				}
			}

			if stra, ok := n.n.AsSource.Select.(*tree.SelectClause); ok {
				tableStra := stra.From.Tables[0]
				if tableName, ok := tableStra.(*tree.AliasedTableExpr).Expr.(*tree.TableName); ok {
					strName := tableName.String()
					if selectExpr, ok := n.n.AsSource.Select.(*tree.SelectClause); ok {
						desc.ViewQuery = fmt.Sprintf("SELECT %s FROM %s", strings.Join(str, ", "), strName)
						// updatable view only from one table
						if tabDesc, ok := selectExpr.From.Tables[0].(*tree.AliasedTableExpr); ok && tabDesc.As.Alias != "" {
							desc.ViewQuery += fmt.Sprintf(" AS %s", tabDesc.As.Alias)
						}
						if selectExpr.Where != nil {
							whereExprstring := selectExpr.Where.Expr.String()
							desc.ViewQuery += fmt.Sprintf(" WHERE %s", whereExprstring)
						}

					}
				}
				// 为ViewQuery追加orderby
				if n.n.AsSource.OrderBy != nil {
					var orderByStr []string
					for _, orderby := range n.n.AsSource.OrderBy {
						orderByStr = append(orderByStr, orderby.Expr.String())
					}
					desc.ViewQuery += fmt.Sprintf(" ORDER BY %s", strings.Join(orderByStr, ", "))
				}
			}
		} else if !n.n.Materialized {
			for i := range resultColumns {
				str[i] = sqlutil.AddExtraMark(resultColumns[i].Name, sqlutil.DoubleQuotation)
			}
			desc.ViewQuery = fmt.Sprintf("select %s from ( %s )", strings.Join(str, ", "), desc.ViewQuery)
		}
	}

	for i, colRes := range resultColumns {
		colType, err := coltypes.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colType}
		if len(columnNames) > i {
			columnTableDef.Name = columnNames[i]
		}
		columnPrivs := sqlbase.InheritFromTablePrivileges(privileges)

		// Nullability constraints do not need to exist on the view, since they are
		// already enforced on the source data.
		columnTableDef.Nullable.Nullability = tree.SilentNull

		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, &params.p.semaCtx, columnPrivs, id)
		if colRes.VisibleTypeName != "" && col != nil {
			col.Type.VisibleTypeName = colRes.VisibleTypeName
		}

		// TODO(cms): 如果这是一个可更新视图,需要为每一列的描述符增加其对应的列的名称
		// 不可直接更新的列需要置空， 对应列名由上一层传入
		if updatable {
			if !(colRes.VisibleTypeName == "row_num") {
				col.UpdatableViewDependsColName = columnsReflec[i]
			}
		}

		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}
	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := desc.AllocateIDs()
	desc.MaxColumnID = int32(len(desc.Columns) + 2)
	return desc, err
}

// MakeViewTableDesc returns the table descriptor for a new view.
func MakeViewTableDesc(
	n *tree.CreateView,
	resultColumns sqlbase.ResultColumns,
	parentID, id sqlbase.ID,
	creationTime hlc.Timestamp,
	privileges *sqlbase.PrivilegeDescriptor,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
) (sqlbase.MutableTableDescriptor, error) {
	viewName := n.Name.Table()
	desc := InitTableDescriptor(id, parentID, viewName, creationTime, privileges, n.Temporary)
	desc.ViewQuery = tree.AsStringWithFlags(n.AsSource, tree.FmtParsable)

	for i, colRes := range resultColumns {
		colType, err := coltypes.DatumTypeToColumnType(colRes.Typ)
		if err != nil {
			return desc, err
		}
		columnTableDef := tree.ColumnTableDef{Name: tree.Name(colRes.Name), Type: colType}
		if len(n.ColumnNames) > i {
			columnTableDef.Name = n.ColumnNames[i]
		}

		colPrivs := sqlbase.InheritFromTablePrivileges(desc.Privileges)
		// The new types in the CREATE VIEW column specs never use
		// SERIAL so we need not process SERIAL types here.
		col, _, _, err := sqlbase.MakeColumnDefDescs(&columnTableDef, semaCtx, colPrivs, desc.ID)
		if err != nil {
			return desc, err
		}
		desc.AddColumn(*col)
	}
	// AllocateIDs mutates its receiver. `return desc, desc.AllocateIDs()`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := desc.AllocateIDs()
	return desc, err
}

const invalidBaseID sqlbase.ID = 0
const (
	viewAlter  uint8 = 0
	viewCreate uint8 = 1
)

// analyzeViewUpdatable analyzes if a view is updatable or not
// 判断视图是否满足可更新视图的需求, 返回该源表id以及视图列名与原表的映射关系（可更新视图只有一个源表)
// 返回值: 是否为可更新视图 / 源表ID / 视图与源表列名的映射关系1 / 视图与源表列名的映射关系2
//	backRefMutables and hasStar is useful only when flag is viewCreate
func analyzeViewUpdatable(
	p *planner,
	planDeps map[sqlbase.ID]planDependencyInfo,
	n *tree.Select,
	flag uint8,
	backRefMutables map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	hasStar bool,
) (bool, sqlbase.ID, []string, []string) {
	// sign that if this table is updatable
	updatable := false
	// sign that which table/view it depends on
	baseID := invalidBaseID
	baseView := false
	// hasAggregateOrWindow to check if a selectClause contains aggregate function / window function or not
	hasAggregateOrWindow := func(s *tree.SelectClause) bool {
		for _, columns := range s.Exprs {
			if funcD, ok := columns.Expr.(*tree.FuncExpr); ok {
				if funcD.GetAggregateConstructor() != nil || funcD.IsWindowFunctionApplication() {
					return true
				}
			}
		}
		return false
	}

	// hasForbiddenDefinition to check if a selectClause contains group by, having or others couldn't appear in updatable view
	hasForbiddenDefinition := func(s *tree.SelectClause) bool {
		if s.Distinct || s.Having != nil || s.GroupBy != nil {
			return true
		}
		return false
	}

	// first: view is just based on only a table or another updatable view
	if len(planDeps) != 1 {
		return false, invalidBaseID, nil, nil
	}
	// second: columns of view should not contain aggregate functions and window functions
	// third: can not use these clause at top of select
	/*
		group by, having, limit (fetch first/next n row/rows only), offset,
		distinct, with  , union, intersect, except
	*/
	if n.Limit != nil { // limit / offset
		return false, invalidBaseID, nil, nil
	}
	if n.With != nil {
		return false, invalidBaseID, nil, nil
	}
	wrapped := n.Select
	if sel, ok := wrapped.(*tree.SelectClause); ok {
		if sel.From != nil && sel.From.Tables != nil {
			selTable := sel.From.Tables[0]
			if AliasedTable, ok := selTable.(*tree.AliasedTableExpr); ok {
				if aliasedTableSubQuery, ok := AliasedTable.Expr.(*tree.Subquery); ok {
					if parenSelect, ok := aliasedTableSubQuery.Select.(*tree.ParenSelect); ok {
						wrapped = parenSelect.Select.Select
						return false, invalidBaseID, nil, nil
					}
				}
			}
		}
	}
	switch s := wrapped.(type) {
	case *tree.SelectClause:
		if hasAggregateOrWindow(s) || hasForbiddenDefinition(s) {
			return false, invalidBaseID, nil, nil
		}
		for i := range s.From.Tables {
			if _, ok := s.From.Tables[i].(*tree.JoinTableExpr); ok {
				return false, invalidBaseID, nil, nil
			}
		}
	case *tree.UnionClause: // union, except, intersect are both classified in tree.UnionClause
		return false, invalidBaseID, nil, nil
	default:
		return false, invalidBaseID, nil, nil
	}
	// planDeps has only a value now
	updatable = true
	for _, values := range planDeps {
		if values.desc.DependsOn != nil {
			baseID = values.desc.ID
			baseView = true
		} else {
			baseID = values.desc.ID
		}
	}
	// get view column reflect slice
	columnsReflect, columnsReflectWithoutRowid := getViewColumnReflect(p, planDeps, updatable, baseView, baseID, flag, backRefMutables, hasStar)

	return updatable, baseID, columnsReflect, columnsReflectWithoutRowid
}

func makeColumnReflect(p *planner, columnsReflectMap map[string]string, baseView bool) []string {
	var exprs tree.SelectExprs
	var s tree.SelectStatement
	columnsReflect := make([]string, 0)

	switch t := p.stmt.AST.(type) {
	case *tree.AlterViewAS:
		s = t.AsSource.Select
	case *tree.CreateView:
		s = t.AsSource.Select
	default:
		return nil
	}
	if columnName, ok := s.(*tree.SelectClause); ok {
		exprs = columnName.Exprs
	} else {
		return nil
	}

	for i := range exprs {
		// 带前缀的列名
		if columnExpr, ok := exprs[i].Expr.(*tree.UnresolvedName); ok {
			if columnsReflectMap[columnExpr.Parts[0]] != "" {
				if baseView {
					columnsReflect = append(columnsReflect, columnsReflectMap[columnExpr.Parts[0]])
				} else {
					columnsReflect = append(columnsReflect, columnExpr.Parts[0])
				}
			} else {
				columnsReflect = append(columnsReflect, "")
			}
		} else {
			columnsReflect = append(columnsReflect, "")
		}
	}
	return columnsReflect
}

// getViewColumnReflect returns two slice
// 函数返回两个切片, 分别表示可更新视图的列与源表列名的对应关系.
// 		first  :	view column reflect slice.
//		second :	view column reflect slice without "rowid" column.
func getViewColumnReflect(
	p *planner,
	planDeps map[sqlbase.ID]planDependencyInfo,
	updatable bool,
	baseView bool,
	baseID sqlbase.ID,
	flag uint8,
	backRefMutables map[sqlbase.ID]*sqlbase.MutableTableDescriptor,
	hasStar bool,
) ([]string, []string) {
	if !updatable {
		return nil, nil
	}
	columnsReflect := make([]string, 0)
	columnsReflectWithoutRowid := make([]string, 0) // columnsReflectWithoutRowid用于构建select *时的列名
	columnsReflectMap := make(map[string]string)

	if flag == viewAlter {
		backRefMutableTables := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor, len(planDeps))

		for id, updated := range planDeps {
			backrefID := updated.desc.ID
			backRefMutable := p.Tables().getUncommittedTableByID(backrefID).MutableTableDescriptor
			if backRefMutable == nil {
				backRefMutable = sqlbase.NewMutableExistingTableDescriptor(*updated.desc.TableDesc())
			}
			backRefMutableTables[id] = backRefMutable
		}
		columnsRefDesc := backRefMutableTables[baseID]
		columnsReflectName := columnsRefDesc.GetColumns()
		if baseView {
			// 读取 base视图的所有列名以及每个列名(columnsReflectName[i].Name)它基于的上一级列名(UpdatableViewDependsColName)
			for i := range columnsReflectName {
				columnsReflectMap[columnsReflectName[i].Name] = columnsReflectName[i].UpdatableViewDependsColName
			}
		} else {
			for i := range columnsReflectName {
				columnsReflectMap[columnsReflectName[i].Name] = columnsReflectName[i].Name
			}
		}
		columnsReflect = makeColumnReflect(p, columnsReflectMap, baseView)

	} else {
		// 整理源表所有列的名称, 根据创建view的语句进行相应的调整, 没有被涉及到的列需要置空值
		// 基于视图创建视图
		if baseView {
			// 应用于带*号的创建
			for id := range backRefMutables {
				columnsReflectDesc := backRefMutables[id]
				columnsReflectName := columnsReflectDesc.GetColumns()
				// 读取 base视图的所有列名以及每个列名(columnsReflectName[i].Name)它基于的上一级列名(UpdatableViewDependsColName)
				for i := range columnsReflectName {
					columnsReflect = append(columnsReflect, columnsReflectName[i].UpdatableViewDependsColName)
					//物化视图中包含rowid列
					if !columnsReflectName[i].Hidden {
						columnsReflectWithoutRowid = append(columnsReflectWithoutRowid, columnsReflectName[i].Name)
					}
					columnsReflectMap[columnsReflectName[i].Name] = columnsReflectName[i].UpdatableViewDependsColName
				}
				if !hasStar {
					columnsReflect = makeColumnReflect(p, columnsReflectMap, baseView)
				}
			}
		} else { //基于表创建视图
			columnsReflectDesc := backRefMutables[baseID]
			columnsReflectName := columnsReflectDesc.GetColumns()
			for i := range columnsReflectName {
				columnsReflect = append(columnsReflect, columnsReflectName[i].Name)
				columnsReflectMap[columnsReflectName[i].Name] = columnsReflectName[i].Name
				if !columnsReflectName[i].Hidden {
					columnsReflectWithoutRowid = append(columnsReflectWithoutRowid, columnsReflectName[i].Name)
				}
			}
			if !hasStar {
				columnsReflect = makeColumnReflect(p, columnsReflectMap, baseView)
			}
		}
	}

	return columnsReflect, columnsReflectWithoutRowid
}
