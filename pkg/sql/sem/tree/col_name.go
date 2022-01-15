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

package tree

import (
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
)

// GetRenderColName computes a name for a result column.
// A name specified with AS takes priority, otherwise a name
// is derived from the expression.
//
// This function is meant to be used on untransformed syntax trees.
//
// The algorithm is borrowed from FigureColName() in PostgreSQL 10, to be
// found in src/backend/parser/parse_target.c. We reuse this algorithm
// to provide names more compatible with PostgreSQL.
func GetRenderColName(searchPath sessiondata.SearchPath, target SelectExpr) (string, error) {
	if target.As != "" {
		return string(target.As), nil
	}

	_, s, err := ComputeColNameInternal(searchPath, target.Expr, false)
	if err != nil {
		return s, err
	}
	if len(s) == 0 {
		s = "?column?"
	}
	return s, nil
}

// ComputeColNameInternal is the workhorse for GetRenderColName.
// The return value indicates the strength of the confidence in the result:
// 0 - no information
// 1 - second-best name choice
// 2 - good name choice
//
// The algorithm is borrowed from FigureColnameInternal in PostgreSQL 10,
// to be found in src/backend/parser/parse_target.c.
func ComputeColNameInternal(
	sp sessiondata.SearchPath, target Expr, casesensitive bool,
) (int, string, error) {
	// The order of the type cases below mirrors that of PostgreSQL's
	// own code, so that code reviews can more easily compare the two
	// implementations.
	switch e := target.(type) {
	case *UnresolvedName:
		if e.Star {
			return 0, "", nil
		}
		return 2, e.Parts[0], nil

	case *ColumnItem:
		return 2, e.Column(), nil

	case *IndirectionExpr:
		return ComputeColNameInternal(sp, e.Expr, casesensitive)

	case *FuncExpr:
		fd, _ := e.Func.Resolve(sp, casesensitive)
		// TODO: commen here may cause big problem
		// if err != nil {
		// 	return 0, "", err
		// }
		return 2, fd.Name, nil

	case *NullIfExpr:
		return 2, "nullif", nil

	case *IfExpr:
		return 2, "if", nil

	case *ParenExpr:
		return ComputeColNameInternal(sp, e.Expr, casesensitive)

	case *CastExpr:
		strength, s, err := ComputeColNameInternal(sp, e.Expr, casesensitive)
		if err != nil {
			return 0, "", err
		}
		if strength <= 1 {
			// Note: this is not exactly correct because it should use the
			// PostgreSQL-internal type name which ZNBaseDB does not
			// implement. However this is close enough.
			var tName string
			if showType, ok := e.Type.(coltypes.T); ok {
				tName = strings.ToLower(showType.ColumnType())
			} else {
				tName = strings.ToLower(e.Type.TypeName())
			}
			// TTuple has no short time name, so check this
			// here. Otherwise we'll want to fall back below.
			if tName != "" {
				return 1, tName, nil
			}
		}
		return strength, s, nil

	case *AnnotateTypeExpr:
		// Ditto CastExpr.
		strength, s, err := ComputeColNameInternal(sp, e.Expr, casesensitive)
		if err != nil {
			return 0, "", err
		}
		if strength <= 1 {
			// Note: this is not exactly correct because it should use the
			// PostgreSQL-internal type name which ZNBaseDB does not
			// implement. However this is close enough.
			var tName string
			if showType, ok := e.Type.(coltypes.T); ok {
				tName = strings.ToLower(showType.ColumnType())
			} else {
				tName = strings.ToLower(e.Type.TypeName())
			}
			// TTuple has no short time name, so check this
			// here. Otherwise we'll want to fall back below.
			if tName != "" {
				return 1, tName, nil
			}
		}
		return strength, s, nil

	case *CollateExpr:
		return ComputeColNameInternal(sp, e.Expr, casesensitive)

	case *ArrayFlatten:
		return 2, "array", nil

	case *Subquery:
		if e.Exists {
			return 2, "exists", nil
		}
		return computeColNameInternalSubquery(sp, e.Select, casesensitive)

	case *CaseExpr:
		strength, s, err := 0, "", error(nil)
		if e.Else != nil {
			strength, s, err = ComputeColNameInternal(sp, e.Else, casesensitive)
		}
		if strength <= 1 {
			s = "case"
			strength = 1
		}
		return strength, s, err

	case *Array:
		return 2, "array", nil

	case *Tuple:
		if e.Row {
			return 2, "row", nil
		}
		if len(e.Exprs) == 1 {
			if len(e.Labels) > 0 {
				return 2, e.Labels[0], nil
			}
			return ComputeColNameInternal(sp, e.Exprs[0], casesensitive)
		}

	case *CoalesceExpr:
		return 2, "coalesce", nil

	case *CharExpr:
		return 2, "char", nil

	case *NvlExpr:
		switch target.(*NvlExpr).Name {
		case "IFNULL":
			return 2, "ifnull", nil
		default:
			return 2, "nvl", nil
		}

		// ZNBaseDB-specific nodes follow.
	case *IfErrExpr:
		if e.Else == nil {
			return 2, "iserror", nil
		}
		return 2, "iferror", nil

	case *ColumnAccessExpr:
		return 2, e.ColName, nil

	case *DBool:
		// PostgreSQL implements the "true" and "false" literals
		// by generating the expressions 't'::BOOL and 'f'::BOOL, so
		// the derived column name is just "bool". Do the same.
		return 1, "bool", nil
	}

	return 0, "", nil
}

// computeColNameInternalSubquery handles the cases of subqueries that
// cannot be handled by the function above due to the Go typing
// differences.
func computeColNameInternalSubquery(
	sp sessiondata.SearchPath, s SelectStatement, casesensitive bool,
) (int, string, error) {
	switch e := s.(type) {
	case *ParenSelect:
		return computeColNameInternalSubquery(sp, e.Select.Select, casesensitive)
	case *ValuesClause:
		if len(e.Rows) > 0 && len(e.Rows[0]) == 1 {
			return 2, "column1", nil
		}
	case *SelectClause:
		if len(e.Exprs) == 1 {
			if len(e.Exprs[0].As) > 0 {
				return 2, string(e.Exprs[0].As), nil
			}
			return ComputeColNameInternal(sp, e.Exprs[0].Expr, casesensitive)
		}
	}
	return 0, "", nil
}

// ColName represents column name and its table name.
type ColName struct {
	ColName   Name
	TableName TableName
}

// Format implements the NodeFormatter interface.
func (col *ColName) Format(ctx *FmtCtx) {
	col.TableName.Format(ctx)
	ctx.WriteByte('(')
	ctx.FormatNode(&col.ColName)
	ctx.WriteByte(')')
}

// MakeColNameWithTable creates a new fully qualified column name.
func MakeColNameWithTable(db, schema, tbl, col Name) ColName {
	return ColName{
		ColName: col,
		TableName: TableName{
			tblName{
				TableName: tbl,
				TableNamePrefix: TableNamePrefix{
					CatalogName:     db,
					SchemaName:      schema,
					ExplicitSchema:  true,
					ExplicitCatalog: true,
				},
			},
		},
	}
}
