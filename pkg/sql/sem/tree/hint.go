package tree

import (
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/keys"
)

// Hint represents a hint
type Hint interface {
	// 两种获取不同类型hint的接口
	GetIndexHint() *IndexHint
	GetTableHint() *TableHint
	GetHintType() keys.HintType

	// 将hint还原成string的接口
	FormatHint() string

	IsSame(h Hint) bool
}

// IndexHint 代表一个表使用index的情况, 一个表仅可以使用一个hint: USE_INDEX(tablename, index_name...)
type IndexHint struct {
	IndexHintType keys.IndexHintType
	TableName     TableName
	IndexNames    TableIndexNames
}

func (i *IndexHint) setTable(table Name) {
	i.TableName = TableName{tblName{TableName: table}}
}

// IsSame return true if IndexHint are same
func (i *IndexHint) IsSame(h Hint) (res bool) {
	if h.GetHintType() == keys.TableLevelHint {
		return false
	}
	other := h.GetIndexHint()
	res = res && other.IndexHintType != i.IndexHintType
	res = res && other.TableName == other.TableName
	for _, index := range i.IndexNames {
		found := false
		for _, oIndex := range other.IndexNames {
			if index.String() == oIndex.String() {
				found = true
				break
			}
		}
		res = res && found
	}
	return
}

// GetIndexHint return index hint
func (i *IndexHint) GetIndexHint() *IndexHint {
	return i
}

// GetTableHint return nil
func (i *IndexHint) GetTableHint() *TableHint {
	return nil
}

// GetHintType return index hint type
func (i *IndexHint) GetHintType() keys.HintType {
	return keys.IndexLevelHint
}

// FormatHint 返回格式化的index hint.
func (i *IndexHint) FormatHint() string {
	var builder strings.Builder
	switch i.IndexHintType {
	case keys.IndexHintForce:
		fmt.Fprintf(&builder, "force_index(%s", i.TableName.Table())
	}
	for _, idnexName := range i.IndexNames {
		fmt.Fprintf(&builder, " %s", idnexName.String())
	}
	fmt.Fprintf(&builder, ")")
	return builder.String()
}

// TableHint represents table-level hint
type TableHint struct {
	HintName string
	HintData interface{} // 括号里的值, 如 merge_join(t1, t2) 就是t1, t2.
	QBName   string
	Tables   []TableName
	Index    []string
}

func (t *TableHint) setTable(old, new Name) {
	for i, tbl := range t.Tables {
		if tbl.TableName == old {
			t.Tables[i] = TableName{tblName{TableName: new}}
		}
	}
}

// FormatHint 返回格式化的table hint.
func (t *TableHint) FormatHint() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s(", t.HintName)
	if t.QBName != "" {
		fmt.Fprintf(&builder, "%s", t.QBName)
	}
	switch t.HintName {
	case "force_index":
		fmt.Fprintf(&builder, " %s,", t.Tables[0].Table())
		fmt.Fprintf(&builder, "%s", strings.Join(t.Index, ","))
	case "merge_join":
		for _, table := range t.Tables {
			fmt.Fprintf(&builder, " %s", table.Table())
		}
	}
	fmt.Fprintf(&builder, ")")
	return builder.String()
}

// IsSame return true if TableHint are same
func (t *TableHint) IsSame(h Hint) (res bool) {
	if h.GetHintType() != keys.TableLevelHint {
		res = false
		return
	}
	other := h.GetTableHint()
	res = res && t.HintName == other.HintName
	res = res && t.QBName == other.QBName
	for _, index := range t.Index {
		found := false
		for _, oIndex := range other.Index {
			if oIndex == index {
				found = true
				break
			}
		}
		res = res && found
	}
	for _, table := range t.Tables {
		found := false
		for _, oTable := range other.Tables {
			if oTable == table {
				found = true
				break
			}
		}
		res = res && found
	}
	return
}

// GetIndexHint return nil.
func (t *TableHint) GetIndexHint() *IndexHint {
	return nil
}

// GetTableHint return TableHint.
func (t *TableHint) GetTableHint() *TableHint {
	return t
}

// GetHintType return hint type.
func (t *TableHint) GetHintType() keys.HintType {
	return keys.TableLevelHint
}

// HintSet represent a slice hint
type HintSet []Hint

// GetHintInfo 将一个HintSet中的index hint和table hint 分开
func (h HintSet) GetHintInfo(
	defQBName string,
	tablehints map[string]map[string][]*TableHint,
	indexhints map[string]map[string][]*IndexHint,
	fromTables TableExprs,
) (map[string]map[string][]*TableHint, map[string]map[string][]*IndexHint) {
	if tablehints == nil {
		tablehints = make(map[string]map[string][]*TableHint, 0)
	}
	if indexhints == nil {
		indexhints = make(map[string]map[string][]*IndexHint, 0)
	}
	for _, table := range fromTables {

		var aliasedTable, aliasedRight Name
		var t, rightTable *TableName
		switch tb := table.(type) {
		case *AliasedTableExpr:
			aliasedTable = tb.As.Alias
			if subQuery, ok := tb.Expr.(*Subquery); ok {
				if s, ok := subQuery.Select.(*ParenSelect).Select.Select.(*SelectClause); ok {
					h.GetHintInfo(defQBName, tablehints, indexhints, s.From.Tables)
				}
			}
			if tn, ok := tb.Expr.(*TableName); ok {
				t = tn
			}
		case *JoinTableExpr:
			if t, ok := tb.Left.(*ParenTableExpr); ok {
				if l, ok := t.Expr.(*JoinTableExpr); ok {
					h.GetHintInfo(defQBName, tablehints, indexhints, append(make(TableExprs, 0), l))
				}
			}
			if left, ok := tb.Left.(*AliasedTableExpr); ok {
				aliasedTable = left.As.Alias
				if subQuery, ok := left.Expr.(*Subquery); ok {
					if s, ok := subQuery.Select.(*ParenSelect).Select.Select.(*SelectClause); ok {
						h.GetHintInfo(defQBName, tablehints, indexhints, s.From.Tables)
					}
				}
				if tb, ok := left.Expr.(*TableName); ok {
					t = tb
				}
			}

			if t, ok := tb.Right.(*ParenTableExpr); ok {
				if r, ok := t.Expr.(*JoinTableExpr); ok {
					h.GetHintInfo(defQBName, tablehints, indexhints, append(make(TableExprs, 0), r))
				}
			}
			if right, ok := tb.Right.(*AliasedTableExpr); ok {
				aliasedRight = right.As.Alias
				if subQuery, ok := right.Expr.(*Subquery); ok {
					if s, ok := subQuery.Select.(*ParenSelect).Select.Select.(*SelectClause); ok {
						h.GetHintInfo(defQBName, tablehints, indexhints, s.From.Tables)
					}
				}
				if tn, ok := right.Expr.(*TableName); ok {
					rightTable = tn
				}
			}
		}
		for i, hint := range h {
			// 暂时index_hint支持全局变量, 也就是index hint不存在query block name, 整个查询里的index hint同处于default中.
			// todo: 考虑👈index hint 的query block name.
			if hint.GetHintType() == keys.IndexLevelHint && t != nil {
				if indexhints["default"] == nil {
					indexhints["default"] = make(map[string][]*IndexHint)
				}
				if hint.GetIndexHint().TableName.Table() == aliasedTable.String() {
					h[i].(*IndexHint).setTable(t.TableName)
					indexhints["default"][t.Table()] = append(indexhints["default"][t.Table()], hint.GetIndexHint())
				} else if aliasedTable.Normalize() != "" && hint.GetIndexHint().TableName.Table() == t.Table() {
					h[i].(*IndexHint).setTable(aliasedTable)
					indexhints["default"][aliasedTable.String()] = append(indexhints["default"][aliasedTable.String()], hint.GetIndexHint())
				} else {
					indexhints["default"][hint.GetIndexHint().TableName.Table()] = append(indexhints["default"][hint.GetIndexHint().TableName.Table()], hint.GetIndexHint())
				}
			} else {
				QBName := defQBName
				for _, tbl := range hint.GetTableHint().Tables {
					if name := hint.GetTableHint().QBName; name != "" {
						QBName = name
					}
					if tablehints[QBName] == nil {
						tablehints[QBName] = make(map[string][]*TableHint)
					}

					// like select /*+ merge_join(tt) */ * from t1 tt, t2;
					if tbl.Table() == aliasedTable.String() && t != nil {
						tablehints[QBName][t.Table()] = append(tablehints[QBName][t.Table()], hint.GetTableHint())

						// like select /*+ merge_join(t1) */ * from t1 tt, t2;
					} else if aliasedTable.Normalize() != "" && t != nil && tbl.Table() == t.Table() {
						h[i].GetTableHint().setTable(tbl.TableName, aliasedTable)
						tablehints[QBName][aliasedTable.String()] = append(tablehints[QBName][aliasedTable.String()], hint.GetTableHint())

						// like select /*+ merge_join(tt) */ * from t1 join t2 tt on t1.a=t2.c1;
					} else if tbl.Table() == aliasedRight.String() && rightTable != nil {
						tablehints[QBName][rightTable.Table()] = append(tablehints[QBName][rightTable.Table()], hint.GetTableHint())

						// like select /*+ merge_join(t2) */ * from t1 join t2 tt on t1.a=t2.c1;
					} else if aliasedRight.Normalize() != "" && rightTable != nil && tbl.Table() == rightTable.Table() {
						h[i].GetTableHint().setTable(tbl.TableName, aliasedRight)
						tablehints[QBName][aliasedRight.String()] = append(tablehints[QBName][aliasedRight.String()], hint.GetTableHint())

						// no aliased table name
					} else if rightTable != nil && tbl.Table() == rightTable.Table() || t != nil && tbl.Table() == t.Table() {
						tablehints[QBName][tbl.Table()] = append(tablehints[QBName][tbl.Table()], hint.GetTableHint())
					}
				}
			}
		}
	}
	return tablehints, indexhints
}

// Format implements the NodeFormatter interface.
func (t *TableHint) Format(ctx *FmtCtx) {
	ctx.WriteString(t.HintName)
	ctx.WriteString("(")
	switch t := t.HintData.(type) {
	case TableNames:
		ctx.FormatNode(&t)
	case int:
		ctx.WriteString(fmt.Sprint(t))
	}
	ctx.WriteString(")")
}

// Format implements the NodeFormatter interface.
func (i *IndexHint) Format(ctx *FmtCtx) {
	if i.IndexHintType == keys.IndexHintUse {
		ctx.WriteString("USE_INDEX")
	}
	if i.IndexHintType == keys.IndexHintIgnore {
		ctx.WriteString("IGNORE_INDEX")
	}
	if i.IndexHintType == keys.IndexHintForce {
		ctx.WriteString("FORCE_INDEX")
	}
	ctx.WriteString("(")
	ctx.FormatNode(&i.TableName)
	ctx.WriteString(", ")
	ctx.FormatNode(&i.IndexNames)
	ctx.WriteString(")")
}

// ReadFromStorage represents hint of read_from_storage.
type ReadFromStorage struct {
	EngineType string
	TableNames TableNames
}

// Format implements the NodeFormatter interface.
func (r *ReadFromStorage) Format(ctx *FmtCtx) {
	ctx.WriteString(r.EngineType)
	ctx.FormatNode(&r.TableNames)
}

// ReadFromStorageList represents  hint of read_from_storage list.
type ReadFromStorageList []*ReadFromStorage

const (
	defaultUpdateBlockName   = "upd_1"
	defaultDeleteBlockName   = "del_1"
	defaultSelectBlockPrefix = "sel_"
)

// GenerateQBName 暂时支持sel
func GenerateQBName(nodeType string, blockOffset int) string {
	if blockOffset == 0 {
		if nodeType == "delete" {
			return defaultDeleteBlockName
		}
		if nodeType == "update" {
			return defaultUpdateBlockName
		}
	}
	return fmt.Sprintf("%s%d", defaultSelectBlockPrefix, blockOffset)
}
