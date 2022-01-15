package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/log"
)

const (
	//TriggerTypeRow  1
	TriggerTypeRow = 1 << 0
	//TriggerTypeStatement  2
	TriggerTypeStatement = 1 << 1
	//TriggerTypeBefore  4
	TriggerTypeBefore = 1 << 2
	//TriggerTypeAfter  8
	TriggerTypeAfter = 1 << 3
	//TriggerTypeInstead  16
	TriggerTypeInstead = 1 << 4
	//TriggerTypeInsert  32
	TriggerTypeInsert = 1 << 5
	//TriggerTypeDelete  64
	TriggerTypeDelete = 1 << 6
	//TriggerTypeUpdate  128
	TriggerTypeUpdate = 1 << 7
	//TriggerTypeTruncate  256
	TriggerTypeTruncate = 1 << 8
)

type createTriggerNode struct {
	n             *tree.CreateTrigger
	tabDesc       *sqlbase.TableDescriptor
	rowTimeEvents int64
	expr          string
	funcid        tree.DOid
}

//Createtrig checks the validity of the trigger sql statement and preprocesses some of the data
func (p *planner) CreateTrigger(ctx context.Context, n *tree.CreateTrigger) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support trigger now!")
	}
	var RowTimeEvents int64     // use 8 bits indicate time,events and row or statement
	var db, sch, proname string // 需要绑定的存储过程的db, schema, name 和 args信息
	var funcid tree.DOid        // 需要绑定的存储过程的ID

	tableDesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Tablename, false, requireTableDesc)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		return nil, fmt.Errorf("table '%s' does not exist", string(n.Tablename.TableName))
	}

	//Check the TRIGGER privilege
	if err := p.CheckPrivilege(ctx, tableDesc, privilege.TRIGGER); err != nil {
		return nil, err
	}

	// 检查绑定的存储过程是否包含操作自身的DDL,例如 alter table X add column b string / drop table X
	// 这是不允许的, 删除表所在的schema或者database也是不被允许的
	// 如果存储过程是以 create trigger T before insert on TABLE for each row execute procedure PRO() 的形式绑定的
	// 需要在更后面的代码处才能取得存储过程的内容, 也需要对应进行判断
	// 在触发器真正执行的过程中也加入了是否修改自身的判断, 详见function IfTableBoundToTrigger() in file 'sp_internal.go'
	isLegalProcedureForTrigger := func(procedure string) error {
		eachSqls := strings.Split(procedure, ";")
		for i := 0; i < len(eachSqls); i++ {
			isDDlForThisTable := func(astP parser.Statement, err error) bool {
				if err != nil {
					return false
				}
				fixCatalogAndSche := func(name tree.TableName) tree.TableName {
					if name.CatalogName == "" {
						name.CatalogName = n.Tablename.CatalogName
					}
					if name.SchemaName == "" {
						name.SchemaName = n.Tablename.SchemaName
					}
					return name
				}
				astT := astP.AST // may astP be empty ?
				var ok = false
				switch t := astT.(type) {
				case *tree.CreateIndex:
					targetTableName := fixCatalogAndSche(t.Table)
					ok = targetTableName.Equals(&n.Tablename)
				case *tree.AlterIndex:
					targetTableName := fixCatalogAndSche(t.Index.Table)
					ok = targetTableName.Equals(&n.Tablename)
				case *tree.AlterTable:
					targetTableName := fixCatalogAndSche(t.Table)
					ok = targetTableName.Equals(&n.Tablename)
				case *tree.RenameTable:
					targetTableName := fixCatalogAndSche(t.Name)
					ok = targetTableName.Equals(&n.Tablename)
				case *tree.Truncate:
					for _, name := range t.Tables {
						name = fixCatalogAndSche(name)
						if name.Equals(&n.Tablename) {
							ok = true
							break
						}
					}
				case *tree.DropTable:
					for _, name := range t.Names {
						name = fixCatalogAndSche(name)
						if name.Equals(&n.Tablename) {
							ok = true
							break
						}
					}
				case *tree.DropIndex:
					for _, index := range t.IndexList {
						name := fixCatalogAndSche(index.Table)
						if name.Equals(&n.Tablename) {
							ok = true
							break
						}
					}
				case *tree.DropSchema:
					for _, schemaName := range t.Schemas {
						if (schemaName.Catalog() == "" || schemaName.Catalog() == n.Tablename.Catalog()) &&
							schemaName.Schema() == n.Tablename.Schema() {
							ok = true
							break
						}
					}
				case *tree.DropDatabase:
				case *tree.RenameDatabase:
					if t.Name.String() == n.Tablename.Catalog() {
						ok = true
					}
				case *tree.AlterSchema:
					if (t.Schema.Catalog() == "" || t.Schema.Catalog() == n.Tablename.Catalog()) &&
						t.Schema.Schema() == n.Tablename.Schema() {
						ok = true
					}
				default:
				}
				return ok
			}
			var astSQL parser.Statement
			var err error
			var eachSQL string
			var newI int
			flag := false
			for newI = i; newI < len(eachSqls); newI++ {
				// 由于 分号 ; 可以出现在字符串中, 因此凭借;进行sql语句的划分是不准确的
				// 当一句SQL语句不符合语法时,在此考虑与后一个划分合并进行解析
				eachSQL += eachSqls[newI]
				astSQL, err = parser.ParseOne(eachSQL, p.EvalContext().GetCaseSensitive())
				if err == nil {
					flag = true
					break
				}
			}
			if flag {
				// 如果flag == false, 说明该句话可能是存储过程内的语法,例如游标,动态SQL等,此时不该跳过中间拼接的部分
				i = newI
			}

			if ok := isDDlForThisTable(astSQL, err); ok {
				return fmt.Errorf("procedure for trigger can not contains DDL which will modify this table")
			}
		}
		return nil
	}
	if n.HasProcedure {
		if err := isLegalProcedureForTrigger(n.Procedure); err != nil {
			return nil, err
		}
	}

	// 如果该create trigger含有begin end部分，则根据内容现行建立一个存储过程，并补充完整n的结构
	// 该存储过程的schema与添加触发器的表相同
	// 该存储过程名字暂定为 usr + tblName + queryID[0:17](sessionId)
	if n.HasProcedure {
		// construct a unique name for procedure
		nameToCreate := p.SessionData().User + n.Tablename.Table() + p.stmt.queryID.String()[0:17] + "trigger_for"
		name := "\"" + DoubleQM(string(n.Tablename.CatalogName)) + "\".\"" +
			DoubleQM(string(n.Tablename.SchemaName)) + "\".\"" +
			DoubleQM(nameToCreate) + "\""
		createProcedure := "create or replace procedure %s() as $$ " + "begin %s end $$ language plpgsql"
		createProcedure = fmt.Sprintf(createProcedure, name, n.Procedure)
		_, err := p.extendedEvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).Exec(ctx, "createProcedureForTrigger", p.txn, createProcedure)
		if err != nil {
			return nil, err
		}
		db = n.Tablename.Catalog()
		sch = n.Tablename.Schema()
		proname = nameToCreate
	}

	// 绑定的存储过程如果没有写定schema和database, 存储过程需要根据search_path进行寻找
	if !n.HasProcedure {
		pronames, ok := n.FuncName.FunctionReference.(*tree.UnresolvedName)
		if ok {
			switch pronames.NumParts {
			case 1:
				db = p.CurrentDatabase()
				if len(p.CurrentSearchPath().GetPathArray()) == 0 {
					return nil, fmt.Errorf("search_path is empty, please check it")
				}
				sch = p.CurrentSearchPath().GetPathArray()[0]
				proname = pronames.Parts[0]
			case 2:
				db = p.CurrentDatabase()
				sch = pronames.Parts[1]
				proname = pronames.Parts[0]
			case 3:
				db = pronames.Parts[2]
				sch = pronames.Parts[1]
				proname = pronames.Parts[0]
			}
		}
	}
	var pronames = "\"" + db + "\".\"" + sch + "\".\"" + proname + "\""

	// test procedure descriptor
	allFuncDescs, err := GetAllFuncDescs(ctx, p.txn)
	if err != nil {
		return nil, fmt.Errorf("it occurs some error, please try again")
	}
	unResolvedNameForProcedure := tree.MakeUnresolvedName(db, sch, proname)
	funcName := tree.MakeTableNameFromUnresolvedName(&unResolvedNameForProcedure)
	dbDesc, err := p.ResolveUncachedDatabase(ctx, &funcName)
	if err != nil {
		return nil, fmt.Errorf("database for procedure does not exist")
	}
	scheDesc, err := dbDesc.GetSchemaByName(sch)
	if err != nil {
		return nil, fmt.Errorf("schema for procedure does not exist")
	}
	overloads, ids := fromAllDescGetOverloads(allFuncDescs, proname, scheDesc.GetID())
	_, fns, indexs, err := tree.ChooseFunctionInOverload(&p.semaCtx, types.Any, n, overloads)
	if err != nil {
		if !n.HasProcedure {
			return nil, fmt.Errorf("procedure %s does not exist", proname)
		}
		return nil, fmt.Errorf("it occurs some error, please try again")
	}
	if len(fns) == 1 {
		funcDesc := allFuncDescs[ids[indexs[0]]]
		// 判断存储过程里是否包含操作该表的DDL
		// begin后的内容才是需要继续检测的部分, declare和begin中间的部分应该忽略
		pro := strings.TrimSpace(funcDesc.FuncDef)
		indexOfBegin := strings.Index(strings.ToLower(pro), "begin")
		if indexOfBegin < 0 || indexOfBegin+5 >= len(pro) {
			log.Warning(ctx, fmt.Sprintf("procedure %d's descriptor happens some error", funcDesc.ID))
			return nil, fmt.Errorf("some error happens when get procedure desc, please try again")
		}
		if err := isLegalProcedureForTrigger(pro[indexOfBegin+5:]); err != nil {
			return nil, err
		}
		funcid = tree.DOid{
			DInt: tree.DInt(funcDesc.ID),
		}
		n.Funcname = pronames
	} else if len(fns) == 0 {
		return nil, fmt.Errorf("procedures %s does not exist", pronames)
	} else {
		return nil, fmt.Errorf("too many procedures")
	}

	var flag = false // indicate truncate event
	for _, event := range n.Events {
		if event == "truncate" {
			flag = true
			RowTimeEvents = RowTimeEvents | TriggerTypeTruncate
		}
		if event == "insert" {
			RowTimeEvents = RowTimeEvents | TriggerTypeInsert
		}
		if event == "update" {
			RowTimeEvents = RowTimeEvents | TriggerTypeUpdate
		}
		if event == "delete" {
			RowTimeEvents = RowTimeEvents | TriggerTypeDelete
		}
	}

	if !tableDesc.IsView() {
		if n.Timing == "instead" {
			return nil, fmt.Errorf("tables or partitioned tables can't have INSTEAD OF triggers")
		}
		if tableDesc.LocationNums > 0 {
			if n.Row && n.Timing == "before" {
				return nil, fmt.Errorf("partitioned tables can't have BEFORE/FOR EACH ROW triggers")
			}
		}
	} else {
		if n.Timing != "instead" && n.Row {
			return nil, fmt.Errorf("views can't have row-level BEFORE or AFTER triggers")
		}
		if flag {
			return nil, fmt.Errorf("views can't have TRUNCATE triggers")
		}
	}

	for _, fk := range tableDesc.AllNonDropIndexes() {
		if fk.ForeignKey.IsSet() {
			if n.Timing == "instead" {
				return nil, fmt.Errorf("foreign tables can't have INSTEAD OF triggers")
			}
			if flag {
				return nil, fmt.Errorf("foreign tables can't have TRUNCATE triggers")
			}
		}
	}

	if n.Row && flag {
		return nil, fmt.Errorf("TRUNCATE FOR EACH ROW triggers are not supported")
	}

	if n.Timing == "instead" {
		if !n.Row {
			return nil, fmt.Errorf("INSTEAD OF triggers must be FOR EACH ROW")
		}
		if n.WhenClause != nil {
			return nil, fmt.Errorf("INSTEAD OF triggers cannot have WHEN conditions")
		}
	}

	if !n.Row && n.WhenClause != nil {
		return nil, fmt.Errorf("statement-level triggers cannot have WHEN conditions")
	}

	if n.Row {
		RowTimeEvents = RowTimeEvents | TriggerTypeRow
	} else {
		RowTimeEvents = RowTimeEvents | TriggerTypeStatement
	}

	if n.Timing == "before" {
		RowTimeEvents = RowTimeEvents | TriggerTypeBefore
	} else if n.Timing == "instead" {
		RowTimeEvents = RowTimeEvents | TriggerTypeInstead
	} else {
		RowTimeEvents = RowTimeEvents | TriggerTypeAfter
	}

	var expr string

	if n.WhenClause != nil {
		switch t := n.WhenClause.(type) {
		case *tree.ComparisonExpr:
			op := t.Operator
			err := parseExpr(op, t, tableDesc.TableDescriptor, uint32(RowTimeEvents))
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsuport whenexpr")
		}
		expr = tree.Serialize(n.WhenClause)
	}

	return &createTriggerNode{n: n, tabDesc: &tableDesc.TableDescriptor, rowTimeEvents: RowTimeEvents, expr: expr, funcid: funcid}, nil
}

// convert createTriggerNode into key-value save at system.triggers
func (n *createTriggerNode) startExec(params runParams) error {
	k := sqlbase.MakeTrigMetadataKey(n.tabDesc.ID, string(n.n.Trigname))
	b := &client.Batch{}
	val, _ := params.p.txn.Get(params.ctx, k)
	if val.Value != nil {
		return fmt.Errorf("newtrigger's name on table %s already exists", n.tabDesc.Name)
	}

	idx := len(sqlbase.TriggersTable.Columns) - len(sqlbase.TriggersTable.PrimaryIndex.ColumnIDs)
	prIdex := len(sqlbase.TriggersTable.PrimaryIndex.ColumnIDs)

	//make values skip primarykey,value' len is 10
	value := make([]tree.Datum, idx)
	for i := len(sqlbase.TriggersTable.PrimaryIndex.ColumnIDs); i < len(sqlbase.TriggersTable.Columns); i++ {
		//use column.ID get its type and convert datum
		value[i-prIdex] = n.GetValue(uint32(sqlbase.TriggersTable.Columns[i].ID))
	}

	var kvValue roachpb.Value
	var rawValueBuf []byte
	var colDiff = sqlbase.TriggersTable.Columns[prIdex].ID
	var err error

	for i := 0; i < idx; i++ {
		rawValueBuf, err = sqlbase.EncodeTableValue(rawValueBuf, colDiff, value[i], nil)
		if err != nil {
			return err
		}
		colDiff = 1
	}

	kvValue.SetTuple(rawValueBuf)
	b.CPut(k, &kvValue, nil)
	err = params.p.txn.Run(params.ctx, b)
	if err != nil {
		return err
	}
	return nil
}

func (*createTriggerNode) Next(runParams) (bool, error) { return false, nil }
func (*createTriggerNode) Values() tree.Datums          { return tree.Datums{} }
func (*createTriggerNode) Close(context.Context)        {}

func (n *createTriggerNode) GetValue(id uint32) tree.Datum {
	switch id {
	case 1:
		return tree.NewDInt(tree.DInt(n.tabDesc.ID))
	case 2:
		return tree.NewDString(string(n.n.Trigname))
	case 3:
		return tree.NewDInt(n.funcid.DInt)
	case 4:
		return tree.NewDInt(tree.DInt(n.rowTimeEvents))
	case 5:
		return tree.DBoolFalse // enable/disable
	case 6:
		return tree.MakeDBool(tree.DBool(n.n.Isconstraint))
	case 7:
		return tree.NewDString("default") //constrname
	case 8:
		return tree.NewDInt(0) //constrrelid
	case 9:
		return tree.MakeDBool(tree.DBool(n.n.Deferrable))
	case 10:
		return tree.MakeDBool(tree.DBool(n.n.Initdeferred))
	case 11:
		return tree.NewDInt(tree.DInt(len(n.n.Args)))
	case 12:
		str := make([]tree.Datum, 0)
		for i := 0; i < len(n.n.Args); i++ {
			var value string
			if cas, ok := n.n.Args[i].(*tree.CastExpr); ok {
				value = cas.Expr.String()
			} else {
				value = n.n.Args[i].String()
			}
			str = append(str, tree.NewDBytes(tree.DBytes(value)))
		}
		return &tree.DArray{
			ParamTyp: types.String,
			Array:    str,
			HasNulls: false,
		}
	case 13:
		return tree.NewDBytes(tree.DBytes(n.expr))
	case 14:
		return tree.MakeDBool(tree.DBool(n.n.HasProcedure))
	}
	return nil
}

//func (n *createTriggerNode)GetFuncID(name string)uint32{
//	return 1
//}

func parseExpr(
	op tree.ComparisonOperator,
	t *tree.ComparisonExpr,
	tabledesc sqlbase.TableDescriptor,
	byte uint32,
) error {
	lstar, lType, err := walkExpr(op, t.Left, tabledesc, byte)
	if err != nil {
		return err
	}
	rstar, rType, err := walkExpr(op, t.Right, tabledesc, byte)
	if err != nil {
		return err
	}
	if lstar && !rstar || !lstar && rstar {
		return fmt.Errorf("unmatched synax at '*'")
	}
	if lType != rType {
		return fmt.Errorf("unmatched type")
	}
	return nil
}

func walkExpr(
	op tree.ComparisonOperator, expr tree.Expr, tabledesc sqlbase.TableDescriptor, byte uint32,
) (bool, types.T, error) {
	switch t := expr.(type) {

	case *tree.UnresolvedName:

		if t.Parts[1] == "old" && byte&TriggerTypeInsert == TriggerTypeInsert {
			return false, nil, fmt.Errorf("INSERT trigger's WHEN condition cannot reference OLD values")
		} else if t.Parts[1] == "new" && byte&TriggerTypeDelete == TriggerTypeDelete {
			return false, nil, fmt.Errorf("DELETE trigger's WHEN condition cannot reference NEW values")
		} else if t.Parts[1] == "" {
			return false, nil, fmt.Errorf("reference '%s' is ambiguous", t.Parts[0])
		} else if !(t.Parts[1] == "old" || t.Parts[1] == "new") {
			return false, nil, fmt.Errorf("unsupport keyword '%s'", t.Parts[1])
		}

		if t.Star {
			if !(op == tree.IsDistinctFrom || op == tree.IsNotDistinctFrom) {
				return false, nil, fmt.Errorf("unsuport synax at '*'")
			}
			return true, nil, nil
		}

		for col := range tabledesc.Columns {
			if t.Parts[0] == tabledesc.Columns[col].Name {
				recol := sqlbase.ResultColumnsFromColDescs(tabledesc.Columns)
				return false, recol[col].Typ, nil
			}
			if col == len(tabledesc.Columns)-1 {
				return false, nil, fmt.Errorf("col '%s' does not exist", t.Parts[0])
			}
		}

	default:
		var ctx *tree.SemaContext
		ctype, _ := t.TypeCheck(ctx, types.Any, false)
		restype := ctype.ResolvedType()
		return false, restype, nil

	}

	return false, nil, fmt.Errorf("unexpect type")
}
