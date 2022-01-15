package optbuilder

import (
	"go/constant"
	"strings"

	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/opt/memo"
	"github.com/znbasedb/znbase/pkg/sql/opt/props"
	"github.com/znbasedb/znbase/pkg/sql/opt/props/physical"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// add cluster setting
var enableOracleRecursive = settings.RegisterBoolSetting(
	"sql.opt.optbuilder.startwith.enabled",
	"if set, enable Oracle's Recursive function",
	false,
)

var (
	//CycleStopFlag is a flag use for stopping recursive when there is a loop in data.
	CycleStopFlag bool
	//AllData store the data when recursive.
	//AllData []interface{}
)

const (
	level                  = "level"
	pseudoLevel            = "pseudolevel"
	connectByRoot          = "connect_by_root"
	connectByIsCycle       = "connect_by_iscycle"
	pseudoConnectByIsCycle = "pseudoconnect_by_iscycle"
	recursiveTemp          = "recursive_temp"
	sysConnectByPath       = "sys_connect_by_path"
	connectByIsLeaf        = "connect_by_isleaf"
	pseudoConnectByIsLeaf  = "pseudoconnect_by_isleaf"
	//DELIMITER is delimiter for StartWithvirtualPathColumn column
	DELIMITER = "||startwithvirtualpathcolumn+123456||"
)

func (b *Builder) buildCTE(
	cte *tree.CTE, inScope *scope, isRecursive bool, stmt *tree.Select,
) (memo.RelExpr, physical.Presentation) {
	var (
		AsName                       tree.Name
		nameList                     []string
		columns                      []scopeColumn
		from                         *tree.From
		repeatName                   string
		format                       string
		isStar                       bool
		isFromJoin                   bool
		isMultiple                   bool
		isSubJoin                    bool
		isRootError                  bool
		isPathError                  bool
		isRecursiveEnable            bool
		isExitLevelColumn            bool
		isExitConnectByIsLeafColumn  bool
		isExitConnectByIsCycleColumn bool
		isSame                       bool
		isExitRowid                  bool
	)
	recursiveAsName := "host"

	if !isRecursive {
		cteScope := b.buildStmt(cte.Stmt, nil, inScope)
		cteScope.removeHiddenCols()
		b.dropOrderingAndExtraCols(cteScope)
		return cteScope.expr, b.getCTECols(cteScope, cte.Name)
	}

	evalCtx := b.GetFactoty()

	//get cluster setting flag : sql.opt.optbuilder.with
	if enableOracleRecursive.Get(&evalCtx.Settings.SV) {
		isRecursiveEnable = true
	}

	cteScope := inScope.push()
	withID := b.factory.Memo().NextWithID()
	cteSrc := &cteSource{
		id:   withID,
		name: cte.Name,
	}

	if stmt != nil && stmt.With != nil {
		b.evalCtx.RecursiveData = new(tree.StartWithRecursiveData)
		stmt.With.RecursiveData = new(tree.StartWithRecursiveData)

		if stmt.With.StartWith == true {
			if selClause, ok := stmt.Select.(*tree.SelectClause); ok {
				leftSelectClause, rightSelectClause, isSuccess, Nocycle, priorCol, tblCol, operator := assertion(stmt)

				//judge from'expr whether is join
				isFromJoin, from, AsName, isSubJoin, isMultiple = resoulveFromExpr(leftSelectClause, selClause)

				if isFromJoin {
					//StartWithIsJoin use for rebuilding fromscope in buildSelectClause
					leftSelectClause.StartWithIsJoin = true
					if selClause.Where != nil {
						reconstructWhereExpr(selClause.Where.Expr)
					}
				}

				if isMultiple {
					if selClause.Where != nil {
						selClause.Where = nil
					}
				}

				if selClause.Exprs != nil && isSuccess {
					for key := range from.Tables {
						outScope := b.buildDataSource(from.Tables[key], nil /* indexFlags */, nil, inScope)
						for key, value := range outScope.cols {
							switch string(value.name) {
							case level:
								isExitLevelColumn = true
							case connectByIsLeaf:
								isExitConnectByIsLeafColumn = true
							case connectByIsCycle:
								isExitConnectByIsCycleColumn = true
							case "rowid":
								isExitRowid = true
							}
							if string(value.name) != "rowid" {
								columns = append(columns, outScope.cols[key])
							}
						}
						if isSubJoin {
							for key, value := range columns {
								for i := key; i < len(columns)-1; i++ {
									if value.name == columns[i+1].name {
										format = "column ambiguously defined"
										break
									}
								}
							}
						}
					}

					for key, value := range columns {
						for i := key + 1; i < len(columns); i++ {
							if value.name == columns[i].name {
								isSame = true
							}
						}
					}

					if isSame {
						leftSelectClause.StartWithIsJoin = true
					} else {
						leftSelectClause.StartWithIsJoin = false
					}

					if !isSame {
						if whereExpr, ok := rightSelectClause.Where.Expr.(*tree.ComparisonExpr); ok {
							if l, ok := whereExpr.Left.(*tree.UnresolvedName); ok {
								if find := strings.Contains(l.Parts[0], "."); find {
									a := strings.Split(l.Parts[0], ".")
									l.Parts[0] = a[1]
								}
							}
							if r, ok := whereExpr.Right.(*tree.UnresolvedName); ok {
								if find := strings.Contains(r.Parts[0], "."); find {
									a := strings.Split(r.Parts[0], ".")
									r.Parts[0] = a[1]
								}
							}
						}
						if find := strings.Contains(priorCol, "."); find {
							a := strings.Split(priorCol, ".")
							priorCol = a[1]
						}
						if find := strings.Contains(tblCol, "."); find {
							a := strings.Split(tblCol, ".")
							tblCol = a[1]
						}
					}

					if isSame && leftSelectClause.Where != nil {
						reconstructWhereExpr(leftSelectClause.Where.Expr)
						if stmt.OrderBy != nil {
							//change OrderBy'ecpr, name = t1.id
							for _, value := range stmt.OrderBy {
								name, ok := value.Expr.(*tree.UnresolvedName)
								if ok {
									if name.NumParts == 2 {
										name.Parts[0] = name.Parts[1] + "." + name.Parts[0]
										name.NumParts = 1
									}
								}
							}
						}
					}
					//if isSame && selClause.Where != nil {
					//	reconstructWhereExpr(selClause.Where.Expr)
					//}

					priorColID, talColID, name, left, right, tempExprs := fillTblExpr(columns, priorCol, tblCol, recursiveAsName, selClause.Exprs, isFromJoin, isMultiple, isSame)
					b.evalCtx.RecursiveData.PriorColID = priorColID
					b.evalCtx.RecursiveData.TalColID = talColID

					//fill Table col name
					stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols[:0], name...)
					leftSelectClause.Exprs = append(leftSelectClause.Exprs[:0], left...)
					rightSelectClause.Exprs = append(rightSelectClause.Exprs[:0], right...)

					tblname := tree.MakeUnqualifiedTableName(recursiveTemp)

					//fill added col name,include LEVEL,CONNECT_BY_ROOT,SYS_CONNECT_BY_PATH,CONNECT_BY_ISCYCLE,CONNECT_BY_ISLEAF
					for key, value := range selClause.Exprs {
						//get SYS_CONNECT_BY_PATH information
						Name := getConnectByPathInfo(value.Expr)

						//change column'name avoid appear same column' name
						repeatName = changeName(Name, key)

						switch t := value.Expr.(type) {
						case tree.UnqualifiedStar:
							//starID = key
							isStar = true
						case *tree.FuncExpr:
							switch Name {
							//construct CONNECT_BY_ROOT expression :CONNECT_BY_ROOT(NAME) ROOT_NAME
							case connectByRoot:
								isRootError, format = sysConnectByRootError(t.Exprs)
								ex, left, right := fillConnectByRootExpr(repeatName, AsName, 1, isSame, t.Exprs)
								stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(repeatName))
								selClause.Exprs[key].Expr = ex
								if selClause.Exprs[key].As == "" {
									selClause.Exprs[key].As = connectByRoot
								}
								leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
								rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)

								//construct SYS_CONNECT_BY_PATH expression
							case sysConnectByPath:
								isPathError, format = sysConnectByPathError(t.Exprs)
								ex, left, right := fillConnectByPathExpr(repeatName, recursiveAsName, AsName, isSame, t.Exprs)
								stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(repeatName))
								selClause.Exprs[key].Expr = ex
								if selClause.Exprs[key].As == "" {
									selClause.Exprs[key].As = sysConnectByPath
								}
								leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
								rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)
							}
						case *tree.BinaryExpr:
							name, exprs := resolveBinaryExprName(t)
							repeatName = changeName(name, key)
							switch name {
							case sysConnectByPath:
								isPathError, format = sysConnectByPathError(exprs)
								ex, left, right := fillConnectByPathExpr(repeatName, recursiveAsName, AsName, isSame, exprs)
								stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(repeatName))
								selClause.Exprs[key].Expr = changeFuncExpr(selClause.Exprs[key].Expr, ex)
								leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
								rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)
							case connectByRoot:
								isRootError, format = sysConnectByRootError(exprs)
								ex, left, right := fillConnectByRootExpr(repeatName, AsName, 1, isSame, exprs)
								stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(repeatName))
								selClause.Exprs[key].Expr = changeFuncExpr(selClause.Exprs[key].Expr, ex)
								leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
								rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)
							}

						case *tree.UnresolvedName:
							ColName := t.Parts[0]
							if isSame {
								for _, value := range columns {
									if find := strings.Contains(string(value.name), "."); find {
										tem := strings.Split(string(value.name), ".")
										if ColName == tem[1] {
											if selClause.Exprs[key].As == "" {
												selClause.Exprs[key].As = tree.UnrestrictedName(ColName)
											}
											t.NumParts = 1
											t.Parts[0] = t.Parts[1] + "." + t.Parts[0]
											break
										}
									}
								}
							}
							if value.Expr.(*tree.UnresolvedName).Star {
								isStar = true
							}

							switch ColName {
							case connectByRoot:
								isExist := isNameExit(nameList, ColName)
								if isExist {
									continue
								} else {
									nameList = append(nameList, ColName)
								}
								stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(ColName))
								_, left, right := fillConnectByRootExpr(Name, tree.Name(value.As.String()), 2, isSame, nil)
								leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
								rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)
								selClause.Exprs[key].As = ""
								//construct CONNECT_BY_ISCYCLE expression
							case connectByIsCycle:
								isExist := isNameExit(nameList, ColName)
								if isExist {
									continue
								} else {
									nameList = append(nameList, ColName)
								}

								//Select query column or use recursive function by setting cluster setting
								if isExitConnectByIsCycleColumn && isRecursiveEnable == false {
									//do nothing
								} else if isExitConnectByIsCycleColumn && isRecursiveEnable {
									stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(pseudoConnectByIsCycle))
									b.evalCtx.RecursiveData.CycleStmt = true
									left, right := fillConnectByIsCycleExpr()
									leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
									rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)
									b.evalCtx.RecursiveData.CycleID = len(rightSelectClause.Exprs) - 1
									for key := range selClause.Exprs {
										switch t := selClause.Exprs[key].Expr.(type) {
										case *tree.UnresolvedName:
											if t.Parts[0] == connectByIsCycle {
												t.Parts[0] = pseudoConnectByIsCycle
												selClause.Exprs[key].As = connectByIsCycle
											}
										}
									}
								} else {
									stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(ColName))
									b.evalCtx.RecursiveData.CycleStmt = true
									left, right := fillConnectByIsCycleExpr()
									leftSelectClause.Exprs = append(leftSelectClause.Exprs, left)
									rightSelectClause.Exprs = append(rightSelectClause.Exprs, right)
									b.evalCtx.RecursiveData.CycleID = len(rightSelectClause.Exprs) - 1
								}
							}
						}
					}
					//if select t.* or select *, reconstruct select'expr
					if isStar {
						for _, value := range tempExprs {
							selClause.Exprs = fillSelExpr(selClause.Exprs, value)
						}
					}

					//construct CONNECT_BY_ISLEAF expression
					if isExitConnectByIsLeafColumn && isRecursiveEnable == false {
						//do nothing
					} else if isExitLevelColumn && isRecursiveEnable {
						stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(pseudoConnectByIsLeaf))
						levelLeft, levelRight := fillIsLeafExpr(pseudoConnectByIsLeaf)
						leftSelectClause.Exprs = append(leftSelectClause.Exprs, levelLeft)
						rightSelectClause.Exprs = append(rightSelectClause.Exprs, levelRight)
						b.evalCtx.RecursiveData.IsLeafID = len(rightSelectClause.Exprs) - 1
						for key := range selClause.Exprs {
							switch t := selClause.Exprs[key].Expr.(type) {
							case *tree.UnresolvedName:
								if t.Parts[0] == connectByIsLeaf {
									t.Parts[0] = pseudoConnectByIsLeaf
									selClause.Exprs[key].As = connectByIsLeaf
								}
							}
						}
						if selClause.Where != nil {
							if whereExpr, ok := selClause.Where.Expr.(*tree.ComparisonExpr); ok {
								if l, ok := whereExpr.Left.(*tree.UnresolvedName); ok {
									if find := strings.Contains(l.Parts[0], connectByIsLeaf); find {
										l.Parts[0] = pseudoConnectByIsLeaf
									}
								}
								if r, ok := whereExpr.Right.(*tree.UnresolvedName); ok {
									if find := strings.Contains(r.Parts[0], connectByIsLeaf); find {
										r.Parts[0] = pseudoConnectByIsLeaf
									}
								}
							}
						}
					} else {
						stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(connectByIsLeaf))
						levelLeft, levelRight := fillIsLeafExpr(connectByIsLeaf)
						leftSelectClause.Exprs = append(leftSelectClause.Exprs, levelLeft)
						rightSelectClause.Exprs = append(rightSelectClause.Exprs, levelRight)
						b.evalCtx.RecursiveData.IsLeafID = len(rightSelectClause.Exprs) - 1
					}

					//construct LEVEL expression
					//Select query column or use recursive function by setting cluster setting
					if isExitLevelColumn && isRecursiveEnable == false {
						//do nothing
					} else if isExitLevelColumn && isRecursiveEnable {
						stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(pseudoLevel))
						levelLeft, levelRight := fillLevelExpr(pseudoLevel)
						leftSelectClause.Exprs = append(leftSelectClause.Exprs, levelLeft)
						rightSelectClause.Exprs = append(rightSelectClause.Exprs, levelRight)
						for key := range selClause.Exprs {
							switch t := selClause.Exprs[key].Expr.(type) {
							case *tree.UnresolvedName:
								if t.Parts[0] == level {
									t.Parts[0] = pseudoLevel
									selClause.Exprs[key].As = level
								}
							}
						}
						if selClause.Where != nil {
							if whereExpr, ok := selClause.Where.Expr.(*tree.ComparisonExpr); ok {
								if l, ok := whereExpr.Left.(*tree.UnresolvedName); ok {
									if find := strings.Contains(l.Parts[0], "level"); find {
										l.Parts[0] = pseudoLevel
									}
								}
								if r, ok := whereExpr.Right.(*tree.UnresolvedName); ok {
									if find := strings.Contains(r.Parts[0], "level"); find {
										r.Parts[0] = pseudoLevel
									}
								}
							}
						}
					} else {
						stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(level))
						levelLeft, levelRight := fillLevelExpr(level)
						leftSelectClause.Exprs = append(leftSelectClause.Exprs, levelLeft)
						rightSelectClause.Exprs = append(rightSelectClause.Exprs, levelRight)
					}

					//Construct StartWithvirtualPathColumn for recursive to stop cycle
					ex, virtualName, virtualLeft, virtualRight := virtualIscycleColumn(priorCol, recursiveAsName, AsName)
					stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name(virtualName))
					leftSelectClause.Exprs = append(leftSelectClause.Exprs, virtualLeft)
					rightSelectClause.Exprs = append(rightSelectClause.Exprs, virtualRight)
					selClause.Exprs = append(selClause.Exprs, tree.SelectExpr{Expr: ex})
					b.evalCtx.RecursiveData.VirtualColID = len(stmt.With.CTEList[0].Name.Cols) - 1

					//add rowid column
					if isExitRowid {
						stmt.With.CTEList[0].Name.Cols = append(stmt.With.CTEList[0].Name.Cols, tree.Name("rowid"))
						leftSelectClause.Exprs = append(leftSelectClause.Exprs, tree.SelectExpr{
							Expr: constructUnresolvedName(1, false, "rowid", ""),
						})
						rightSelectClause.Exprs = append(rightSelectClause.Exprs, tree.SelectExpr{
							Expr: constructUnresolvedName(2, false, "rowid", recursiveTemp),
						})
					}

					if selClause.StartWith == nil {
						//reconstruct where'expr in order to recursive without START WITH
						leftSelectClause.Where = reconstructWhere(tblCol)
					}

					b.evalCtx.RecursiveData.Nocycle = Nocycle
					b.evalCtx.RecursiveData.RowLength = len(stmt.With.CTEList[0].Name.Cols)
					b.evalCtx.RecursiveData.Operator = operator

					//fill recursive'rightSelectClause.From:   from table1,table2
					rightSelectClause.From.Tables = tree.TableExprs{
						&tree.AliasedTableExpr{Expr: &tblname},
						&tree.AliasedTableExpr{
							Expr: &tree.Subquery{
								Select: &tree.ParenSelect{
									Select: &tree.Select{
										Select: &tree.SelectClause{
											Exprs: tree.SelectExprs{
												tree.SelectExpr{Expr: &tree.UnqualifiedStar{}},
											},
											From:            leftSelectClause.From,
											StartWithIsJoin: isSame,
										},
									},
								},
							},
							As: tree.AliasClause{
								Alias: tree.Name(recursiveAsName),
							},
						},
					}

					stmt.With.RecursiveData = b.evalCtx.RecursiveData

					if stmt.With.StartWith {
						selectClause, ok := stmt.Select.(*tree.SelectClause)
						if ok {
							name := stmt.With.CTEList[0].Name.Alias
							recursiveTable := tree.UnresolvedObjectName{
								NumParts: 1,
								Parts:    [3]string{string(name)},
							}
							tbName := recursiveTable.ToTableName()

							//reconstruct From'expr and fill As name
							if AsName != "" {
								selectClause.From.Tables = tree.TableExprs{
									&tree.AliasedTableExpr{
										Expr: &tbName,
										As: tree.AliasClause{
											Alias: AsName,
										},
									},
								}
							} else {
								selectClause.From.Tables = tree.TableExprs{
									&tree.AliasedTableExpr{
										Expr: &tbName,
									},
								}
							}
						}
					}
				}
			}
		}
	}

	// WITH RECURSIVE queries are always of the form:
	//
	//   WITH RECURSIVE name(cols) AS (
	//     initial_query
	//     UNION ALL
	//     recursive_query
	//   )
	//
	// Recursive CTE evaluation (paraphrased from postgres docs):
	//  1. Evaluate the initial query; emit the results and also save them in
	//     a "working" table.
	//  2. So long as the working table is not empty:
	//     * evaluate the recursive query, substituting the current contents of
	//       the working table for the recursive self-reference.
	//     * emit all resulting rows, and save them as the next iteration's
	//       working table.
	//
	// Note however, that a non-recursive CTE can be used even when RECURSIVE is
	// specified (particularly useful when there are multiple CTEs defined).
	// Handling this while having decent error messages is tricky.

	// Generate an id for the recursive CTE reference. This is the id through
	// which the recursive expression refers to the current working table
	// (via WithScan).

	// cteScope allows recursive references to this CTE.

	cteScope.ctes = map[string]*cteSource{cte.Name.Alias.String(): cteSrc}

	initial, recursive, unionAll, ok := b.splitRecursiveCTE(cte.Stmt)
	if !ok {
		// Build this as a non-recursive CTE, but throw a proper error message if it
		// does have a recursive reference.
		format = "recursive query %q does not have the form non-recursive-term UNION ALL recursive-term"
		cteSrc.error(cte.Name.Alias, format)
		return b.buildCTE(cte, cteScope, false, stmt)
	}

	if isRootError {
		//connect_by_root() only support one column.
		cteSrc.error(connectByRoot+"()", format)
		return b.buildCTE(cte, cteScope, false, stmt)
	}

	if isPathError {
		//sys_connect_by_path() only support one column
		cteSrc.error(sysConnectByPath+"()", format)
		return b.buildCTE(cte, cteScope, false, stmt)
	}

	if format != "" {
		cteSrc.error("", format)
		return b.buildCTE(cte, cteScope, false, stmt)
	}

	// Set up an error if the initial part has a recursive reference.
	cteSrc.onRef = func() {
		panic(pgerror.NewErrorf(
			pgcode.Syntax,
			"recursive reference to query %q must not appear within its non-recursive term",
			cte.Name.Alias,
		))
	}
	initialScope := b.buildStmt(initial, nil, cteScope)

	initialScope.removeHiddenCols()
	b.dropOrderingAndExtraCols(initialScope)

	// The properties of the binding are tricky: the recursive expression is
	// invoked repeatedly and these must hold each time. We can't use the initial
	// expression's properties directly, as those only hold the first time the
	// recursive query is executed. We can't really say too much about what the
	// working table contains, except that it has at least one row (the recursive
	// query is never invoked with an empty working table).
	bindingProps := &props.Relational{}
	bindingProps.OutputCols = initialScope.colSet()
	bindingProps.Cardinality = props.AnyCardinality.AtLeast(props.OneCardinality)
	// We don't really know the input row count, except for the first time we run
	// the recursive query. We don't have anything better though.
	bindingProps.Stats.RowCount = initialScope.expr.Relational().Stats.RowCount
	cteSrc.bindingProps = bindingProps

	cteSrc.cols = b.getCTECols(initialScope, cte.Name)

	outScope := inScope.push()

	initialTypes := initialScope.makeColumnTypes()

	// Synthesize new output columns (because they contain values from both the
	// initial and the recursive relations). These columns will also be used to
	// refer to the working table (from the recursive query); we can't use the
	// initial columns directly because they might contain duplicate IDs (e.g.
	// consider initial query SELECT 0, 0).
	for i, c := range cteSrc.cols {
		newCol := b.synthesizeColumn(outScope, c.Alias, initialTypes[i], nil /* expr */, nil /* scalar */)
		cteSrc.cols[i].ID = newCol.id
	}

	// We want to check if the recursive query is actually recursive. This is for
	// annoying cases like `SELECT 1 UNION ALL SELECT 2`.
	numRefs := 0
	cteSrc.onRef = func() {
		numRefs++
	}

	var star bool
	if recSelectClause, ok := recursive.Select.(*tree.SelectClause); ok {
		if len(recSelectClause.Exprs) > 0 {
			expr := recSelectClause.Exprs[0].Expr
			if name, ok := expr.(*tree.UnresolvedName); ok {
				star = name.Star
			}
		}
	}

	var recursiveScope *scope
	if star {
		recursiveScope = b.buildSelect(recursive, noRowLocking, initialTypes, cteScope, nil)
	} else {
		recursiveScope = b.buildSelect(recursive, noRowLocking, initialTypes, cteScope, nil)
	}

	if numRefs == 0 {
		// Build this as a non-recursive CTE.
		cteScope := b.buildSetOp(tree.UnionOp, false /* all */, inScope, initialScope, recursiveScope)
		return cteScope.expr, b.getCTECols(cteScope, cte.Name)
	}

	if numRefs != 1 {
		// We disallow multiple recursive references for consistency with postgres.
		panic(pgerror.NewErrorf(
			pgcode.Syntax,
			"WITH query name %q specified more than once",
			cte.Name.Alias,
		))
	}

	recursiveScope.removeHiddenCols()
	b.dropOrderingAndExtraCols(recursiveScope)

	// We allow propagation of types from the initial query to the recursive
	// query.
	_, propagateToRight := b.checkTypesMatch(initialScope, recursiveScope,
		false, /* tolerateUnknownLeft */
		true,  /* tolerateUnknownRight */
		"UNION",
	)
	if propagateToRight {
		recursiveScope = b.propagateTypes(recursiveScope /* dst */, initialScope /* src */)
	}

	private := memo.RecursiveCTEPrivate{
		Name:          string(cte.Name.Alias),
		WithID:        withID,
		InitialCols:   colsToColList(initialScope.cols),
		RecursiveCols: colsToColList(recursiveScope.cols),
		OutCols:       colsToColList(outScope.cols),
		UnionAll:      unionAll,
	}

	expr := b.factory.ConstructRecursiveCTE(initialScope.expr, recursiveScope.expr, &private)
	return expr, cteSrc.cols
}

// getCTECols returns a presentation for the scope, renaming the columns to
// those provided in the AliasClause (if any). Throws an error if there is a
// mismatch in the number of columns.
func (b *Builder) getCTECols(cteScope *scope, name tree.AliasClause) physical.Presentation {
	presentation := cteScope.makePhysicalProps().Presentation
	if len(presentation) == 0 {
		panic(pgerror.NewErrorf(pgcode.FeatureNotSupported,
			"WITH clause %q does not have a RETURNING clause", tree.ErrString(&name)))
	}

	if name.Cols == nil {
		return presentation
	}

	if len(presentation) != len(name.Cols) {
		panic(pgerror.NewErrorf(pgcode.InvalidColumnReference,
			"source %q has %d columns available but %d columns specified",
			name.Alias, len(presentation), len(name.Cols)))
	}
	for i := range presentation {
		presentation[i].Alias = string(name.Cols[i])
	}
	return presentation
}

// splitRecursiveCTE splits a CTE statement of the form
//   initial_query UNION ALL recursive_query
// into the initial and recursive parts. If the statement is not of this form,
// returns ok=false.
func (b *Builder) splitRecursiveCTE(
	stmt tree.Statement,
) (initial, recursive *tree.Select, all bool, ok bool) {
	sel, ok := stmt.(*tree.Select)
	// The form above doesn't allow for "outer" WITH, ORDER BY, or LIMIT
	// clauses.
	if !ok || sel.With != nil || sel.OrderBy != nil || sel.Limit != nil {
		return nil, nil, false, false
	}
	union, ok := sel.Select.(*tree.UnionClause)
	if !ok || union.Type != tree.UnionOp {
		return nil, nil, false, false
	}
	all = union.All
	return union.Left, union.Right, all, true
}

func assertion(
	sel *tree.Select,
) (*tree.SelectClause, *tree.SelectClause, bool, bool, string, string, tree.ComparisonOperator) {
	if stmtClause, ok := sel.With.CTEList[0].Stmt.(*tree.Select); ok {
		if unionClause, ok := stmtClause.Select.(*tree.UnionClause); ok {
			if leftSelectClause, ok := unionClause.Left.Select.(*tree.SelectClause); ok {
				if rightelectClause, ok := unionClause.Right.Select.(*tree.SelectClause); ok {
					if whereExp, ok := rightelectClause.Where.Expr.(*tree.ComparisonExpr); ok {
						Nocycle := whereExp.Nocycle
						if leftExp, ok := whereExp.Left.(*tree.UnresolvedName); ok {
							if rightExp, ok := whereExp.Right.(*tree.UnresolvedName); ok {
								if leftExp.Parts[1] == "recursive_temp" {
									return leftSelectClause, rightelectClause, true, Nocycle, leftExp.Parts[0], rightExp.Parts[0], whereExp.Operator
								} else if rightExp.Parts[1] == "recursive_temp" {
									return leftSelectClause, rightelectClause, true, Nocycle, rightExp.Parts[0], leftExp.Parts[0], whereExp.Operator
								}
							}
						}
					}
				}
			}
		}
	}
	return nil, nil, false, false, "", "", 100
}

func getConnectByPathInfo(expr tree.Expr) string {
	var name string
	if funcExp, ok := expr.(*tree.FuncExpr); ok {
		if funcName, ok := funcExp.Func.FunctionReference.(*tree.UnresolvedName); ok {
			if funcName.Parts[0] == sysConnectByPath {
				name = sysConnectByPath
			} else if funcName.Parts[0] == connectByRoot {
				name = connectByRoot
			}
		}
		return name
	}
	return ""
}

func fillTblExpr(
	columns []scopeColumn,
	priorCol, tblCol, tableName string,
	exprs tree.SelectExprs,
	isFromJoin, isMultiple, isSame bool,
) (int, int, []tree.Name, []tree.SelectExpr, []tree.SelectExpr, map[int]tree.SelectExprs) {
	var (
		leftSelectClause  []tree.SelectExpr
		rightSelectClause []tree.SelectExpr
		priorColID        int
		talColID          int
		name              []tree.Name
		tempExprs         tree.SelectExprs
		endExprs          map[int]tree.SelectExprs
	)

	endExprs = make(map[int]tree.SelectExprs)

	leftSelectClause = append(leftSelectClause, tree.SelectExpr{
		Expr: tree.UnqualifiedStar{},
	})

	if isSame {
		for key, value := range columns {
			if value.table.TableName != "" {
				columns[key].name = value.table.TableName + "." + value.name
			} else if value.expr != nil {
				switch t := value.expr.(type) {
				case *scopeColumn:
					columns[key].name = t.table.TableName + "." + t.name
				}
			}
		}
	}

	for key, value := range columns {
		if string(value.name) == priorCol {
			priorColID = key
		} else if string(value.name) == tblCol {
			talColID = key
		}

		name = append(name, value.name)
		rightSelectClause = append(rightSelectClause, tree.SelectExpr{
			Expr: constructUnresolvedName(2, false, string(value.name), tableName),
		})
	}

	if isFromJoin || isMultiple {
		for key, val := range exprs {
			tempExprs = tree.SelectExprs{}
			switch v := val.Expr.(type) {
			case *tree.UnresolvedName:
				if v.Star {
					//select t1.*, t2.*
					for _, value := range columns {
						if value.expr != nil {
							scopeColumn, ok := value.expr.(*scopeColumn)
							if ok {
								if v.Parts[1] == scopeColumn.table.TableName.String() {
									if find := strings.Contains(string(value.name), "."); find {
										column := strings.Split(string(value.name), ".")
										tempExprs = append(tempExprs, tree.SelectExpr{
											Expr: constructUnresolvedName(1, false, string(value.name), ""),
											As:   tree.UnrestrictedName(column[1]),
										})
									} else {
										tempExprs = append(tempExprs, tree.SelectExpr{
											Expr: constructUnresolvedName(1, false, string(value.name), ""),
										})
									}
								}
							}
						} else if v.Parts[1] == value.table.TableName.String() {
							if find := strings.Contains(string(value.name), "."); find {
								column := strings.Split(string(value.name), ".")
								tempExprs = append(tempExprs, tree.SelectExpr{
									Expr: constructUnresolvedName(1, false, string(value.name), ""),
									As:   tree.UnrestrictedName(column[1]),
								})
							} else {
								tempExprs = append(tempExprs, tree.SelectExpr{
									Expr: constructUnresolvedName(1, false, string(value.name), ""),
								})
							}
						}
					}
					endExprs[key] = tempExprs
				}
			case tree.UnqualifiedStar:
				//select *
				for _, value := range columns {
					if find := strings.Contains(string(value.name), "."); find {
						column := strings.Split(string(value.name), ".")
						tempExprs = append(tempExprs, tree.SelectExpr{
							Expr: constructUnresolvedName(1, false, string(value.name), ""),
							As:   tree.UnrestrictedName(column[1]),
						})
					} else {
						tempExprs = append(tempExprs, tree.SelectExpr{
							Expr: constructUnresolvedName(1, false, string(value.name), ""),
						})
					}
				}
				endExprs[key] = tempExprs
			}
		}
	} else {
		for key, val := range exprs {
			tempExprs = tree.SelectExprs{}
			switch v := val.Expr.(type) {
			case *tree.UnresolvedName:
				if v.Star {
					for _, value := range columns {
						if value.name.String() != "rowid" {
							if v.NumParts == 1 {
								tempExprs = append(tempExprs, tree.SelectExpr{
									Expr: constructUnresolvedName(1, false, string(value.name), ""),
								})
							} else if v.NumParts == 2 {
								tempExprs = append(tempExprs, tree.SelectExpr{
									Expr: constructUnresolvedName(2, false, string(value.name), v.Parts[1]),
								})
							}
						}
					}
					endExprs[key] = tempExprs
				}
			case tree.UnqualifiedStar:
				//select *
				for _, value := range columns {
					if value.name.String() != "rowid" {
						tempExprs = append(tempExprs, tree.SelectExpr{
							Expr: constructUnresolvedName(1, false, string(value.name), ""),
						})
					}
				}
				endExprs[key] = tempExprs
			}
		}

	}

	return priorColID, talColID, name, leftSelectClause, rightSelectClause, endExprs
}

func fillConnectByRootExpr(
	Name string, AsName tree.Name, number int, isJoin bool, exprs tree.Exprs,
) (tree.Expr, tree.SelectExpr, tree.SelectExpr) {
	var (
		ex                tree.Expr
		leftSelectClause  tree.SelectExpr
		rightSelectClause tree.SelectExpr
		binaryExpr        *tree.BinaryExpr
		colExpr           *tree.UnresolvedName
		symbolName        string
	)
	a := &tree.NumVal{
		Value:      constant.MakeInt64(0),
		OrigString: "0",
	}
	b := &tree.NumVal{
		Value:      constant.MakeInt64(1),
		OrigString: "1",
	}
	if exprs != nil {
		for _, value := range exprs {
			switch t := value.(type) {
			case *tree.UnresolvedName:
				colExpr = t
				if isJoin && colExpr.NumParts == 2 {
					colExpr.NumParts = 1
					colExpr.Parts[0] = colExpr.Parts[1] + "." + colExpr.Parts[0]
				}

				switch t.Parts[0] {
				case level:
					leftSelectClause = tree.SelectExpr{
						Expr: b,
					}
					rightSelectClause = tree.SelectExpr{
						Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
					}
				case connectByIsLeaf:
					leftSelectClause = tree.SelectExpr{
						Expr: a,
					}
					rightSelectClause = tree.SelectExpr{
						Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
					}
				case connectByIsCycle:
					leftSelectClause = tree.SelectExpr{
						Expr: a,
					}
					rightSelectClause = tree.SelectExpr{
						Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
					}
				default:
					leftSelectClause = tree.SelectExpr{
						Expr: t,
					}
					rightSelectClause = tree.SelectExpr{
						Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
					}
				}
			case *tree.StrVal:
				symbolName = t.RawString()
				leftSelectClause = tree.SelectExpr{
					Expr: t,
				}
				rightSelectClause = tree.SelectExpr{
					Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
				}
			case *tree.NumVal:
				leftSelectClause = tree.SelectExpr{
					Expr: t,
				}
				rightSelectClause = tree.SelectExpr{
					Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
				}
			case *tree.FuncExpr:
				leftSelectClause = tree.SelectExpr{
					Expr: t,
				}
				rightSelectClause = tree.SelectExpr{
					Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
				}
			case tree.DNullExtern:
				leftSelectClause = tree.SelectExpr{
					Expr: tree.NewStrVal("Null"),
				}
				rightSelectClause = tree.SelectExpr{
					Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
				}
			case *tree.BinaryExpr:
				binaryExpr = t
			}
		}
	} else {

	}

	switch number {
	case 1:
		//connect_by_root(b) or connect_by_root(b)+1
		if AsName != "" {
			ex = constructUnresolvedName(2, false, Name, string(AsName))
		} else {
			ex = constructUnresolvedName(2, false, Name, recursiveTemp)
		}
		if binaryExpr != nil {
			changeBinaryExprForRoot(binaryExpr, a, b)
			constructBinaryExprNameForJoin(binaryExpr, isJoin)
			leftSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left:     tree.NewBytesStrVal(symbolName),
					Right:    binaryExpr,
				},
			}
			rightSelectClause = tree.SelectExpr{
				Expr: constructUnresolvedName(2, false, Name, recursiveTemp),
			}
		}
		return ex, leftSelectClause, rightSelectClause
	case 2:
		//connect_by_root b
		leftSelectClause = tree.SelectExpr{
			Expr: constructUnresolvedName(1, false, string(AsName), ""),
		}
		rightSelectClause = tree.SelectExpr{
			Expr: constructUnresolvedName(2, false, connectByRoot, recursiveTemp),
		}
		return nil, leftSelectClause, rightSelectClause
	}
	return nil, tree.SelectExpr{}, tree.SelectExpr{}
}

func fillConnectByPathExpr(
	Name string, TableName string, AsName tree.Name, isSame bool, exprs tree.Exprs,
) (tree.Expr, tree.SelectExpr, tree.SelectExpr) {
	var (
		leftSelectClause  tree.SelectExpr
		rightSelectClause tree.SelectExpr
		ex                tree.Expr
		colExpr           *tree.UnresolvedName
		symbolNames       []string
		binaryExpr        *tree.BinaryExpr
	)

	if exprs != nil {
		for _, value := range exprs {
			switch t := value.(type) {
			case *tree.UnresolvedName:
				colExpr = t
				if isSame && colExpr.NumParts == 2 {
					colExpr.NumParts = 1
					colExpr.Parts[0] = colExpr.Parts[1] + "." + colExpr.Parts[0]
				}
			case *tree.StrVal:
				symbolNames = append(symbolNames, t.RawString())
			case *tree.NumVal:
				symbolNames = append(symbolNames, t.String())
			case tree.DNullExtern:
				symbolNames = append(symbolNames, t.String())
			case *tree.BinaryExpr:
				binaryExpr = t
			}
		}
	}

	if AsName != "" {
		ex = constructUnresolvedName(2, false, Name, AsName.String())
	} else {
		ex = constructUnresolvedName(2, false, Name, recursiveTemp)
	}

	if binaryExpr != nil && symbolNames != nil {
		constructBinaryExprNameForJoin(binaryExpr, isSame)
		b := *binaryExpr
		right := b.ReconstructBinary(TableName, &tree.BinaryExpr{
			Operator: tree.Plus,
			Left:     constructUnresolvedName(2, false, level, recursiveTemp),
			Right: &tree.NumVal{
				Value:      constant.MakeInt64(1),
				OrigString: "1",
			},
		})
		changeBinaryExprForPath(binaryExpr)
		leftSelectClause = tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Concat,
				Left:     tree.NewBytesStrVal(symbolNames[0]),
				Right:    binaryExpr,
			},
		}
		rightSelectClause = tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Concat,
				Left: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left:     constructUnresolvedName(2, false, Name, recursiveTemp),
					Right:    tree.NewBytesStrVal(symbolNames[0]),
				},
				Right: &right,
			},
		}
	} else if colExpr != nil && symbolNames != nil {
		switch colExpr.Parts[0] {
		case level:
			leftSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left:     tree.NewBytesStrVal(symbolNames[0]),
					Right:    tree.NewDInt(1),
				},
			}
			rightSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left: &tree.BinaryExpr{
						Operator: tree.Concat,
						Left:     constructUnresolvedName(2, false, Name, recursiveTemp),
						Right:    tree.NewBytesStrVal(symbolNames[0]),
					},
					Right: &tree.BinaryExpr{
						Operator: tree.Plus,
						Left:     constructUnresolvedName(2, false, level, recursiveTemp),
						Right: &tree.NumVal{
							Value:      constant.MakeInt64(1),
							OrigString: "1",
						},
					},
				},
			}
		default:
			leftSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left:     tree.NewBytesStrVal(symbolNames[0]),
					Right:    tree.ConstructCoalesceExpr(colExpr, ""),
				},
			}
			rightSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left: &tree.BinaryExpr{
						Operator: tree.Concat,
						Left:     constructUnresolvedName(2, false, Name, recursiveTemp),
						Right:    tree.NewBytesStrVal(symbolNames[0]),
					},
					Right: tree.ConstructCoalesceExpr(constructUnresolvedName(2, false, colExpr.Parts[0], TableName), TableName),
				},
			}
		}
	} else if symbolNames != nil && len(symbolNames) != 1 {
		if symbolNames[0] == "NULL" {
			leftSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left:     tree.NewBytesStrVal(symbolNames[1]),
					Right:    tree.ConstructCoalesceExpr(colExpr, ""),
				},
			}
		} else {
			leftSelectClause = tree.SelectExpr{
				Expr: &tree.BinaryExpr{
					Operator: tree.Concat,
					Left:     tree.NewBytesStrVal(symbolNames[1]),
					Right:    tree.NewBytesStrVal(symbolNames[0]),
				},
			}
		}
		rightSelectClause = tree.SelectExpr{
			Expr: &tree.BinaryExpr{
				Operator: tree.Concat,
				Left:     constructUnresolvedName(2, false, Name, recursiveTemp),
				Right:    leftSelectClause.Expr,
			},
		}
	} else {
		//fill expr casually avoid panic
		leftSelectClause = tree.SelectExpr{
			Expr: tree.UnqualifiedStar{},
		}
		rightSelectClause = tree.SelectExpr{
			Expr: constructUnresolvedName(2, true, "", TableName),
		}
	}
	return ex, leftSelectClause, rightSelectClause
}

func fillConnectByIsCycleExpr() (tree.SelectExpr, tree.SelectExpr) {
	var leftSelectClause, rightSelectClause tree.SelectExpr

	leftSelectClause = tree.SelectExpr{
		Expr: &tree.NumVal{
			Value:      constant.MakeInt64(0),
			OrigString: "0",
		},
	}
	rightSelectClause = tree.SelectExpr{
		Expr: &tree.NumVal{
			Value:      constant.MakeInt64(0),
			OrigString: "0",
		},
	}
	return leftSelectClause, rightSelectClause
}

func fillLevelExpr(level string) (tree.SelectExpr, tree.SelectExpr) {
	var leftSelectClause, rightSelectClause tree.SelectExpr
	leftSelectClause = tree.SelectExpr{
		Expr: &tree.NumVal{
			Value:      constant.MakeInt64(1),
			OrigString: "1",
		},
	}
	rightSelectClause = tree.SelectExpr{
		Expr: &tree.BinaryExpr{
			Operator: tree.Plus,
			Left:     constructUnresolvedName(2, false, level, recursiveTemp),
			Right: &tree.NumVal{
				Value:      constant.MakeInt64(1),
				OrigString: "1",
			},
		},
	}
	return leftSelectClause, rightSelectClause
}

func fillIsLeafExpr(isLeaf string) (tree.SelectExpr, tree.SelectExpr) {
	var leftSelectClause, rightSelectClause tree.SelectExpr
	leftSelectClause = tree.SelectExpr{
		Expr: &tree.NumVal{
			Value:      constant.MakeInt64(1),
			OrigString: "1",
		},
	}
	rightSelectClause = tree.SelectExpr{
		Expr: &tree.NumVal{
			Value:      constant.MakeInt64(1),
			OrigString: "1",
		},
	}
	return leftSelectClause, rightSelectClause
}

func reconstructExpr(
	key int, selClause tree.SelectExprs, tempExprs tree.SelectExprs,
) tree.SelectExprs {
	switch key {
	case 0:
		selClause = append(tempExprs, selClause[key+1:]...)
		return selClause
	case len(selClause) - 1:
		selClause = append(selClause[:len(selClause)-1], tempExprs...)
		return selClause
	default:
		tempStart := selClause[:key]
		tempEnd := append([]tree.SelectExpr{}, selClause[key+1:]...)
		selClause = append(tempStart, tempExprs...)
		selClause = append(selClause, tempEnd...)
		return selClause
	}
}

func isNameExit(nameList []string, name string) bool {
	if nameList != nil {
		for _, value := range nameList {
			if value == name {
				return true
			}
		}
	}
	return false
}

func changeName(name string, key int) string {
	return name + string(rune(key))
}

func reconstructWhere(tblCol string) *tree.Where {
	return &tree.Where{
		Type: "WHERE",
		Expr: &tree.ComparisonExpr{
			Operator:    tree.IsNotDistinctFrom,
			SubOperator: tree.EQ,
			Left:        constructUnresolvedName(1, false, tblCol, ""),
			Right:       tree.DNull,
		},
	}
}

func resolveBinaryExprName(binaryExpr *tree.BinaryExpr) (string, tree.Exprs) {
	switch r := binaryExpr.Right.(type) {
	case *tree.BinaryExpr:
		name, exprs := resolveBinaryExprName(r)
		return name, exprs
	case *tree.FuncExpr:
		if name, ok := r.Func.FunctionReference.(*tree.UnresolvedName); ok {
			return name.Parts[0], r.Exprs
		}
	}
	switch l := binaryExpr.Left.(type) {
	case *tree.BinaryExpr:
		name, exprs := resolveBinaryExprName(l)
		return name, exprs
	case *tree.FuncExpr:
		if name, ok := l.Func.FunctionReference.(*tree.UnresolvedName); ok {
			return name.Parts[0], l.Exprs
		}
	}
	return "", nil
}

func constructComparisonExprName(comparisonExpr tree.Expr) {
	if comparison, ok := comparisonExpr.(*tree.ComparisonExpr); ok {
		switch r := comparison.Right.(type) {
		case *tree.UnresolvedName:
			if r.NumParts == 2 {
				r.NumParts = 1
				r.Parts[0] = r.Parts[1] + "." + r.Parts[0]
			}
		case *tree.BinaryExpr:
			constructBinaryExprNameForJoin(r, ok)
		case *tree.Subquery:
			constructSubquryExprName(r)
		}

		switch l := comparison.Left.(type) {
		case *tree.ComparisonExpr:
			constructComparisonExprName(l)
		case *tree.UnresolvedName:
			if l.NumParts == 2 {
				l.NumParts = 1
				l.Parts[0] = l.Parts[1] + "." + l.Parts[0]
			}
		case *tree.BinaryExpr:
			constructBinaryExprNameForJoin(l, ok)
		case *tree.Subquery:
			constructSubquryExprName(l)
		}

		//if right, ok := comparison.Right.(*tree.UnresolvedName); ok {
		//	if right.NumParts == 2 {
		//		right.NumParts = 1
		//		right.Parts[0] = right.Parts[1] + "." + right.Parts[0]
		//	}
		//} else if rightBinary, ok := comparison.Right.(*tree.BinaryExpr); ok {
		//	constructBinaryExprName(rightBinary, ok)
		//}
		//if left, ok := comparison.Left.(*tree.ComparisonExpr); ok {
		//	constructComparisonExprName(left)
		//}else {
		//	if left, ok := comparison.Left.(*tree.UnresolvedName); ok{
		//		if left.NumParts == 2 {
		//			left.NumParts = 1
		//			left.Parts[0] = left.Parts[1] + "." + left.Parts[0]
		//		}
		//	} else if leftBinary, ok := comparison.Left.(*tree.BinaryExpr); ok {
		//		constructBinaryExprName(leftBinary, ok)
		//	}
		//}
	}
}
func constructSubquryExprName(SubquryExpr tree.Expr) {
	if subqury, ok := SubquryExpr.(*tree.Subquery); ok {
		switch s := subqury.Select.(type) {
		case *tree.ParenSelect:
			if sel, ok := s.Select.Select.(*tree.SelectClause); ok {
				if sel.Where != nil {
					reconstructWhereExpr(sel.Where.Expr)
				}
				for _, value := range sel.Exprs {
					if name, ok := value.Expr.(*tree.UnresolvedName); ok {
						if name.Star == false && name.NumParts == 2 {
							name.NumParts = 1
							name.Parts[0] = name.Parts[1] + "." + name.Parts[0]
						}
					}
				}
			}
		}
	}
}

func constructAndExprName(andExpr tree.Expr) {
	if and, ok := andExpr.(*tree.AndExpr); ok {
		if _, ok := and.Right.(*tree.ComparisonExpr); ok {
			constructComparisonExprName(and.Right)
		}

		switch l := and.Left.(type) {
		case *tree.AndExpr:
			constructAndExprName(l)
		case *tree.ComparisonExpr:
			constructComparisonExprName(l)
		}

		//if left, ok := and.Left.(*tree.AndExpr); ok {
		//	constructAndExprName(left)
		//}else {
		//	if _, ok := and.Left.(*tree.ComparisonExpr); ok {
		//		constructComparisonExprName(and.Left)
		//	}
		//}
	}
}

func constructBinaryExprNameForJoin(expr tree.Expr, isJoin bool) {
	if isJoin {
		if binary, ok := expr.(*tree.BinaryExpr); ok {
			if right, ok := binary.Right.(*tree.UnresolvedName); ok {
				if right.NumParts != 1 {
					right.NumParts = 1
					right.Parts[0] = right.Parts[1] + "." + right.Parts[0]
				}
			}
			if left, ok := binary.Left.(*tree.BinaryExpr); ok {
				constructBinaryExprNameForJoin(left, isJoin)
			} else {
				if left, ok := binary.Left.(*tree.UnresolvedName); ok {
					if left.NumParts != 1 {
						left.NumParts = 1
						left.Parts[0] = left.Parts[1] + "." + left.Parts[0]
					}
				}
			}
		}
	}
}

func changeBinaryExprForRoot(expr tree.Expr, a, b *tree.NumVal) {
	if binary, ok := expr.(*tree.BinaryExpr); ok {
		if right, ok := binary.Right.(*tree.UnresolvedName); ok {
			switch right.Parts[0] {
			case level:
				binary.Right = b
			case connectByIsLeaf:
				binary.Right = a
			case connectByIsCycle:
				binary.Right = a
			default:
				binary.Right = tree.ConstructCoalesceExpr(right, "")
			}
		} else if right, ok := binary.Right.(*tree.BinaryExpr); ok {
			changeBinaryExprForRoot(right, a, b)
		}
		if left, ok := binary.Left.(*tree.BinaryExpr); ok {
			changeBinaryExprForRoot(left, a, b)
		} else if left, ok := binary.Left.(*tree.UnresolvedName); ok {
			switch left.Parts[0] {
			case level:
				binary.Left = b
			case connectByIsLeaf:
				binary.Left = a
			case connectByIsCycle:
				binary.Left = a
			default:
				binary.Left = tree.ConstructCoalesceExpr(left, "")
			}
		}
	}
}

func changeBinaryExprForPath(expr tree.Expr) {
	if binary, ok := expr.(*tree.BinaryExpr); ok {
		if right, ok := binary.Right.(*tree.UnresolvedName); ok {
			switch right.Parts[0] {
			case level:
				binary.Right = tree.NewDInt(1)
			default:
				binary.Right = tree.ConstructCoalesceExpr(right, "")
			}
		} else if right, ok := binary.Right.(*tree.BinaryExpr); ok {
			changeBinaryExprForPath(right)
		}
		if left, ok := binary.Left.(*tree.BinaryExpr); ok {
			changeBinaryExprForPath(left)
		} else if left, ok := binary.Left.(*tree.UnresolvedName); ok {
			switch left.Parts[0] {
			case level:
				binary.Left = tree.NewDInt(1)
			default:
				binary.Left = tree.ConstructCoalesceExpr(left, "")
			}
		}
	}
}

//construct expr to implement 'a'||sys_connect_by_path() and sys_connect_by_path()||'a'
func changeFuncExpr(expr, ex tree.Expr) tree.Expr {
	if binary, ok := expr.(*tree.BinaryExpr); ok {
		if _, ok := binary.Left.(*tree.FuncExpr); ok {
			binary.Left = ex
		}
		changeFuncExpr(binary.Left, ex)
		if _, ok := binary.Right.(*tree.FuncExpr); ok {
			binary.Right = ex
		}
		changeFuncExpr(binary.Right, ex)
	}
	return expr
}

func constructUnresolvedName(
	numParts int, star bool, colName string, tableName string,
) *tree.UnresolvedName {
	return &tree.UnresolvedName{
		NumParts: numParts,
		Star:     star,
		Parts:    tree.NameParts{colName, tableName},
	}
}

func virtualIscycleColumn(
	priorCol string, TableName string, AsName tree.Name,
) (tree.Expr, string, tree.SelectExpr, tree.SelectExpr) {
	var (
		leftSelectClause  tree.SelectExpr
		rightSelectClause tree.SelectExpr
		ex                tree.Expr
		right             *tree.UnresolvedName
	)
	name := "startwithvirtualpathcolumn"
	if AsName != "" {
		ex = constructUnresolvedName(2, false, name, string(AsName))
		right = constructUnresolvedName(1, false, priorCol, "")
	} else {
		ex = constructUnresolvedName(2, false, name, recursiveTemp)
		right = constructUnresolvedName(1, false, priorCol, "")
	}

	leftSelectClause = tree.SelectExpr{
		Expr: &tree.BinaryExpr{
			Operator: tree.Concat,
			Left:     tree.NewBytesStrVal(DELIMITER),
			Right:    tree.ConstructCoalesceExpr(right, ""),
		},
	}
	rightSelectClause = tree.SelectExpr{
		Expr: &tree.BinaryExpr{
			Operator: tree.Concat,
			Left: &tree.BinaryExpr{
				Operator: tree.Concat,
				Left:     constructUnresolvedName(2, false, name, recursiveTemp),
				Right:    tree.NewBytesStrVal(DELIMITER),
			},
			Right: tree.ConstructCoalesceExpr(constructUnresolvedName(2, false, priorCol, TableName), TableName),
		},
	}
	return ex, name, leftSelectClause, rightSelectClause
}

//Resoulve  From'Expr, example: from TableName/ from table join table/ from table,table/ from (select ... from ...)
func resoulveFromExpr(
	leftSelClause, selClause *tree.SelectClause,
) (bool, *tree.From, tree.Name, bool, bool) {
	var (
		isJoin     bool
		isSubJoin  bool
		isMultiple bool
		from       *tree.From
		AsName     tree.Name
	)
	if len(leftSelClause.From.Tables) == 1 {
		if _, ok := leftSelClause.From.Tables[0].(*tree.JoinTableExpr); ok {
			isJoin = true
			from = leftSelClause.From
		} else if fromExpr, ok := leftSelClause.From.Tables[0].(*tree.AliasedTableExpr); ok {
			if sub, ok := fromExpr.Expr.(*tree.Subquery); ok {
				if parentSel, ok := sub.Select.(*tree.ParenSelect); ok {
					if sel, ok := parentSel.Select.Select.(*tree.SelectClause); ok {
						isJoin, _, _, _, _ := resoulveFromExpr(sel, selClause)
						isSubJoin = isJoin
						from = leftSelClause.From
						//if sel.Where != nil && isSubJoin {
						//	reconstructWhereExpr(sel.Where.Expr)
						//}
					}
				}
			} else if _, ok := fromExpr.Expr.(*tree.TableName); ok {
				from = leftSelClause.From
			}
			AsName = fromExpr.As.Alias
		}
	} else {
		//"from table,table where"  change to "from (select ... from table,table where)"
		if selClause.Where != nil {
			isMultiple = true
			from = &tree.From{
				Tables: tree.TableExprs{
					&tree.AliasedTableExpr{
						Expr: &tree.Subquery{
							Exists: false,
							Select: &tree.ParenSelect{
								Select: &tree.Select{
									Select: &tree.SelectClause{
										Exprs: tree.SelectExprs{{Expr: tree.UnqualifiedStar{}}},
										From: &tree.From{
											Tables: selClause.From.Tables,
										},
										//Where:           tree.NewWhere(tree.AstWhere, reconstructWhereExpr(selClause.Where.Expr)),
										Where:           selClause.Where,
										StartWithIsJoin: false,
									},
								},
							},
						},
					},
				},
			}
			leftSelClause.From = from
		} else {
			isJoin = true
			AsName = ""
			from = leftSelClause.From
		}
	}
	return isJoin, from, AsName, isSubJoin, isMultiple
}

func fillSelExpr(exprs tree.SelectExprs, selectExprs tree.SelectExprs) tree.SelectExprs {
	for key, val := range exprs {
		switch t := val.Expr.(type) {
		case *tree.UnresolvedName:
			if t.Star {
				exprs = reconstructExpr(key, exprs, selectExprs)
				return exprs
			}
		case tree.UnqualifiedStar:
			exprs = reconstructExpr(key, exprs, selectExprs)
			return exprs
		}
	}
	return exprs
}

func (c *cteSource) error(args interface{}, format string) {
	c.onRef = func() {
		panic(pgerror.NewErrorf(
			pgcode.Syntax,
			format,
			args,
		))
	}
}

func reconstructWhereExpr(where tree.Expr) tree.Expr {
	switch t := where.(type) {
	case *tree.ComparisonExpr:
		constructComparisonExprName(t)
	case *tree.AndExpr:
		constructAndExprName(t)
	}
	return where
}

func sysConnectByPathError(exprs tree.Exprs) (bool, string) {
	switch len(exprs) {
	case 2:
		switch exprs[1].(type) {
		case *tree.StrVal:
			break
		default:
			return true, "illegal parameter in %q function"
		}
		switch t := exprs[0].(type) {
		case *tree.BinaryExpr:
			if find := strings.Contains(t.String(), "NULL"); find {
				return true, "can not splice NULL in %q function"
			}
			if find := strings.Contains(t.String(), "\"("); find {
				return true, "Function parameters are temporarily not supported in %q function"
			}
			if find := strings.Contains(t.String(), "connect_by"); find {
				exprs[0] = &tree.BinaryExpr{
					Operator: tree.Plus,
					Left:     tree.NewDInt(1),
					Right: &tree.NumVal{
						Value:      constant.MakeInt64(1),
						OrigString: "1",
					},
				}
				return true, "connect_by_isleaf or connect_by_iscycle are temporarily not supported in %q function"
			}
		case *tree.FuncExpr:
			return true, "Function parameters are temporarily not supported in %q function"
		case *tree.UnresolvedName:
			switch t.Parts[0] {
			case connectByIsCycle:
				t.Parts[0] = level
				return true, " connect_by_iscycle are temporarily not supported in %q function"
			case connectByIsLeaf:
				t.Parts[0] = level
				return true, "connect_by_isleaf  are temporarily not supported in %q function"
			default:
				break
			}
		}
	default:
		return true, "illegal parameter in %q function"
	}
	return false, ""
}

func sysConnectByRootError(exprs tree.Exprs) (bool, string) {
	switch len(exprs) {
	case 1:
		switch t := exprs[0].(type) {
		case *tree.BinaryExpr:
			if find := strings.Contains(t.String(), "NULL"); find {
				return true, "can not splice NULL in %q function"
			}
			if find := strings.Contains(t.String(), "\"("); find {
				return true, "Function parameters are temporarily not supported in %q function"
			}
		case *tree.FuncExpr:
			return true, "Function parameters are temporarily not supported in %q function"
		}
	default:
		return true, "%q invalid number of arguments"
	}
	return false, ""
}
