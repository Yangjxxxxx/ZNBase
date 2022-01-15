package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/lex"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
)

//show triggers by sql,search for table system
func (p *planner) ShowTriggers(ctx context.Context, n *tree.ShowTriggers) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support trigger now!")
	}
	// 展示的是当前database下的所有触发器
	var scName string
	for i, k := range p.CurrentSearchPath().GetPathArray() {
		if i != 0 {
			scName += ","
		}
		scName += "'" + DoubleQMSingle(k) + "'"
	}

	queryStmt := "select DISTINCT * from information_schema.trigger_privileges"
	whereCondition := " where relid in (select oid::int from pg_catalog.pg_class)"
	searchPath := " and relid in(select table_id from zbdb_internal.tables where schema_name in (%s))"
	searchPath = fmt.Sprintf(searchPath, scName)
	return p.delegateQuery(ctx, "SHOW TRIGGERS",
		queryStmt+whereCondition+searchPath, nil, nil)
}

func (p *planner) ShowTriggersOnTable(
	ctx context.Context, n *tree.ShowTriggersOnTable,
) (planNode, error) {
	tabledesc, err := p.ResolveMutableTableDescriptor(ctx, &n.Tablename, false, requireTableDesc)
	if tabledesc == nil {
		return nil, fmt.Errorf("table '%s' does not exist", string(n.Tablename.TableName))
	}
	if err != nil {
		return nil, err
	}
	tableID := tabledesc.ID
	//sql:="select * from system.triggers where relid=" + strconv.Itoa(int(tableID)) + " and relid in (select id from system.namespace where \"parentID\" in (select id from system.namespace)) order by name asc"
	queryStmt := "select DISTINCT * from information_schema.trigger_privileges where relid="
	queryStmt += strconv.Itoa(int(tableID))
	return p.delegateQuery(ctx, "SHOW TRIGGERS", queryStmt, nil, nil)
}

func (p *planner) ShowCreateTrigger(
	ctx context.Context, n *tree.ShowCreateTrigger,
) (planNode, error) {
	if !util.EnableUDR {
		return nil, pgerror.NewError(pgcode.FeatureNotSupported, "not support trigger now!")
	}
	var trigtype int
	var TriggerDesc sqlbase.Trigger
	var showstatement string
	const showCreateTriggerQuery = `
     SELECT %[1]s AS trig_name,
			%[2]s AS table_name,
            %[3]s AS create_statement
       FROM %[4]s.zbdb_internal.create_statements LIMIT 1
`
	tabledesc, err := p.ResolveMutableTableDescriptor(ctx, &n.TableName, false, requireTableDesc)
	if tabledesc == nil {
		return nil, fmt.Errorf("table '%s' does not exist", string(n.TableName.TableName))
	}
	if err != nil {
		return nil, err
	}
	//check the show trigger privilege by trigger privilege
	if err := p.CheckPrivilege(ctx, tabledesc, privilege.TRIGGER); err != nil {
		return nil, err
	}
	tableID := tabledesc.ID
	tgDesc, err := MakeTriggerDesc(ctx, p.txn, tabledesc)
	flag := false
	if tgDesc != nil {
		for _, tg := range tgDesc.Triggers {
			if tg.Tgname == n.Name.String() {
				flag = true
				break
			}
		}
	}
	if !flag {
		return nil, fmt.Errorf("trigger '%s' does not exist", string(n.Name))
	}

	for i := range tgDesc.Triggers {
		if int(tgDesc.Triggers[i].Tgrelid) == int(tableID) && tgDesc.Triggers[i].Tgname == string(n.Name) {
			TriggerDesc = tgDesc.Triggers[i]
			trigtype = int(tgDesc.Triggers[i].Tgtype)
		}
	}
	// 通过绑定的id构建对应绑定的procedure名
	funcDesc, err := sqlbase.GetFunctionDescFromID(ctx, p.txn, sqlbase.ID(TriggerDesc.TgfuncID))
	if err != nil {
		return nil, fmt.Errorf("procedure for trigger %s does not exist", TriggerDesc.Tgname)
	}
	// 需要获取该function所属的db和schema
	if schDesc, err := getSchemaDescByID(ctx, p.txn, funcDesc.ParentID); err == nil {
		var db, sch string
		sch = schDesc.Name
		if dbDesc, err := getDatabaseDescByID(ctx, p.txn, schDesc.ParentID); err == nil {
			db = dbDesc.Name
			TriggerDesc.TgfuncName = db + "." + sch + "." + funcDesc.Name
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}

	splits := strings.Split(TriggerDesc.TgfuncName, ".")
	var proname string
	var procedure string
	if len(splits) == 3 {
		proname = splits[2]
	} else if len(splits) == 2 {
		proname = splits[1]
	} else if len(splits) == 1 {
		proname = splits[0]
	}
	argsTmp := "("
	for i, k := range TriggerDesc.Tgargs {
		if i != 0 {
			argsTmp += ","
		}
		argsTmp += k
	}
	argsTmp += ")"
	if isCreatedByTrigger(proname) == false {
		if TriggerDesc.Tgtype == 0 {
			return nil, fmt.Errorf("trigger '%s' does not exist", string(n.Name))
		}
		showstatement = getShowTriggerStmt(trigtype, n, TriggerDesc, procedure)
	} else {
		global, err := p.ExecCfg().InternalExecutor.Query(ctx, "SHOW CREATE TRIGGERS", p.txn, "select prosrc from system.procedures where proname='"+proname+"'")
		proce := global[0].String()
		proce = proce[2 : len(proce)-2]
		proce = strings.Replace(proce, "begin", "", -1)
		proce = strings.Replace(proce, "end", "", -1)
		procedure = "begin $$\n" + proce + "\n$$ end"
		if err != nil {
			return nil, fmt.Errorf("%s", err)
		}
		if TriggerDesc.Tgtype == 0 {
			return nil, fmt.Errorf("trigger '%s' does not exist", string(n.Name))
		}
		showstatement = getShowTriggerStmt(trigtype, n, TriggerDesc, procedure)
	}
	//sql:="select * from system.triggers where relid=" + strconv.Itoa(int(tableID)) + " and relid in (select id from system.namespace where \"parentID\" in (select id from system.namespace where name = current_schema())) order by name asc"
	//return p.delegateQuery(ctx, "SHOW TRIGGERS",sql,nil,nil)
	//showstatement := "CREATE TRIGGER "+string(n.Name)+"AFTER INSERT ON "+n.TableName.String()+"\nFOR EACH ROW\nEXECUTE PROCEDURE "+TriggerDesc.TgfuncName+argsTmp
	fulltriggerQuery := fmt.Sprintf(showCreateTriggerQuery,
		lex.EscapeSQLString(n.Name.String()),
		lex.EscapeSQLString(n.TableName.String()),
		lex.EscapeSQLString(showstatement),
		n.TableName.CatalogName.String(),
	)
	return p.delegateQuery(ctx, "SHOW CREATE TRIGGERS", fulltriggerQuery, nil, nil)
}

//a function to judge whether a procedure is created by CreateTrigger() according to its proname
func isCreatedByTrigger(proname string) bool {
	Sig := "trigger_for"
	length := len(proname)
	lengthOfSig := len(Sig)
	if length > 43 && proname[length-lengthOfSig:] == Sig {
		return true
	}
	return false
}

func getShowTriggerStmt(
	oid int, n *tree.ShowCreateTrigger, TriggerDesc sqlbase.Trigger, procedure string,
) string {
	inTrigtypeArray := func(id int, tritypOids []int) bool {
		for _, v := range tritypOids {
			if id == v {
				return true
			}
		}
		return false
	}
	var trigtypeOid = []int{37, 69, 133, 261, 41, 73, 137, 265, 49, 81, 145, 273, 38, 70, 134, 262, 42, 74, 138, 266, 50, 82, 146, 274, 101, 165, 197, 105, 169, 201, 113, 177, 209, 229, 233, 241, 102, 166, 198, 106, 170, 202, 114, 178, 210, 230, 234, 242, 294, 326, 390, 358, 422, 454, 486, 298, 330, 394, 362, 426, 458, 490, 306, 338, 402, 370, 434, 466, 498}
	if !inTrigtypeArray(oid, trigtypeOid) {
		return "Unsupported show create trigger show format"
	}

	beforeFlag := (oid & TriggerTypeBefore) != 0
	afterFlag := (oid & TriggerTypeAfter) != 0
	insteadFlag := (oid & TriggerTypeInstead) != 0

	insertFlag := (oid & TriggerTypeInsert) != 0
	deleteFlag := (oid & TriggerTypeDelete) != 0
	updateFlag := (oid & TriggerTypeUpdate) != 0
	truncateFlag := (oid & TriggerTypeTruncate) != 0

	rowFlag := (oid & TriggerTypeRow) != 0
	statementFlag := (oid & TriggerTypeStatement) != 0

	statement := "CREATE TRIGGER " + string(n.Name)

	if beforeFlag {
		statement += " BEFORE"
	} else if afterFlag {
		statement += " AFTER"
	} else if insteadFlag {
		statement += " INSTEAD"
	}

	if insertFlag {
		statement += " INSERT OR"
	}
	if deleteFlag {
		statement += " DELETE OR"
	}
	if updateFlag {
		statement += " UPDATE OR"
	}
	if truncateFlag {
		statement += " TRUNCATE OR"
	}
	statement = statement[:len(statement)-2]
	statement += "ON "
	statement += n.TableName.String()
	if rowFlag {
		statement += "\nFOR EACH ROW\n"
	} else if statementFlag {
		statement += "\nFOR EACH STATEMENT\n"
	}

	if TriggerDesc.Tgwhenexpr != "" {
		statement += "WHEN(" + TriggerDesc.Tgwhenexpr + ")" + "\n"
	}
	statement += procedure
	//fmt.Println(truncateFlag, updateFlag, deleteFlag, insertFlag, insteadFlag, afterFlag, beforeFlag, statementFlag, rowFlag)
	return statement
}
