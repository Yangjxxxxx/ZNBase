// Copyright 2019 The IncloudExDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/parser"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/encoding"
)

// MakeTriggerDesc returns the next available Descriptor ID and increments
// the counter. The incrementing is non-transactional, and the counter could be
// incremented multiple times because of retries.
func MakeTriggerDesc(
	ctx context.Context, txn *client.Txn, descriptor sqlbase.DescriptorProto,
) (*sqlbase.TriggerDesc, error) {
	if !util.EnableUDR {
		return nil, nil
	}

	descKey := sqlbase.MakeTrigMetadataKey(descriptor.GetID(), "")

	kvs, err := txn.Scan(ctx, descKey, descKey.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	tdes := sqlbase.TriggerDesc{}
	tdes.Triggers = make([]sqlbase.Trigger, len(kvs))
	for i, kv := range kvs {
		// get table id and name
		relID, triggerName, err := DecodeTriggerMetadataID(kv.Key)
		if err != nil {
			return nil, err
		}

		tdes.Triggers[i].Tgrelid = tree.ID(relID)
		tdes.Triggers[i].Tgname = triggerName

		// parse values
		valueBytes, err := kv.Value.GetTuple()
		if err != nil {
			return nil, err
		}

		var colIDDiff uint32
		var lastColID sqlbase.ColumnID
		var typeOffset, dataOffset int
		var typ encoding.Type
		for len(valueBytes) > 0 {
			typeOffset, dataOffset, colIDDiff, typ, err = encoding.DecodeValueTag(valueBytes)
			if err != nil {
				return nil, err
			}
			colID := lastColID + sqlbase.ColumnID(colIDDiff)
			lastColID = colID

			var encValue sqlbase.EncDatum
			encValue, valueBytes, err = sqlbase.EncDatumValueFromBufferWithOffsetsAndType(valueBytes, typeOffset,
				dataOffset, typ)
			if err != nil {
				return nil, err
			}

			alloc := &sqlbase.DatumAlloc{}
			err := encValue.EnsureDecoded(&sqlbase.TriggersTable.Columns[colID-1].Type, alloc)
			if err != nil {
				return nil, err
			}
			tdes.Triggers[i].Set(uint32(colID-1), encValue.Datum)
		}
	}

	return &tdes, nil
}

// DecodeTriggerMetadataID decodes a descriptor ID from a descriptor metadata key.
func DecodeTriggerMetadataID(key roachpb.Key) (uint32, string, error) {
	// Extract object ID from key.
	// TODO(marc): move sql/keys.go to keys (or similar) and use a DecodeDescMetadataKey.
	// We should also check proper encoding.
	remaining, systableID, err := keys.DecodeTablePrefix(key)
	if err != nil {
		return 0, "", err
	}
	if systableID != keys.TriggersTableID {
		return 0, "", errors.Errorf("key is not a trigger table entry: %v", key)
	}
	// DescriptorTable.PrimaryIndex.ID
	remaining, _, err = encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, "", err
	}
	// descID
	remaining, tableID, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return 0, "", err
	}

	// name
	_, name, err := encoding.DecodeBytesAscending(remaining, nil)
	if err != nil {
		return 0, "", err
	}

	return uint32(tableID), string(name), nil
}

// ExecuteTriggers run before statement
func ExecuteTriggers(
	ctx context.Context,
	txn *client.Txn,
	params runParams,
	triggerDesc *sqlbase.TriggerDesc,
	tabledesc *sqlbase.ImmutableTableDescriptor,
	sourceVals tree.Datums,
	b *client.Batch,
	eventType uint32,
	runType uint32,
	levelType uint32,
) error {
	if !util.EnableUDR {
		return nil
	}

	// 检测该DML语句是否来自存储过程，如果是，则不触发触发器
	//_, ok := ctx.Value("DmlFromProcedure").(*ProcedureSig)
	//var PM PgManager

	//ok := PM.IfFromProc(ctx)

	if /*ok ||*/ triggerDesc == nil || len(triggerDesc.Triggers) == 0 {
		return nil
	}

	// find trigger of specific type  && call function
	for _, tg := range triggerDesc.Triggers {
		if tg.IsType(eventType, runType, levelType) {
			if runType == TriggerTypeAfter && levelType == TriggerTypeRow {
				if txn != nil {
					err := txn.Run(ctx, b)
					if err != nil {
						return err
					}
					*b = *txn.NewBatch()
				}
			}

			//jiaoyan expr
			if levelType == TriggerTypeStatement || tg.Tgwhenexpr == "" {
				err := Call(ctx, txn, params, tg.Tgargs, int(tg.TgfuncID), tg.Tgname, tg.Tgrelid)
				if err != nil {

					return err
				}
			} else {
				expr, err := parser.ParseExpr(tg.Tgwhenexpr)
				if err != nil {
					panic(err)
				}
				switch t := expr.(type) {
				case *tree.ComparisonExpr:
					op := t.Operator
					rstar, rval, err := getValue(t.Right, tabledesc, sourceVals)
					if err != nil {
						return err
					}
					lstar, lval, err := getValue(t.Left, tabledesc, sourceVals)
					if err != nil {
						return err
					}
					if (rstar || lstar) && op == tree.IsNotDistinctFrom {
						if eventType != TriggerTypeUpdate {
							err := Call(ctx, txn, params, tg.Tgargs, int(tg.TgfuncID), tg.Tgname, tg.Tgrelid)
							if err != nil {
								return err
							}
							continue
						}
					}
					if (rstar || lstar) && op == tree.IsDistinctFrom {
						if eventType != TriggerTypeUpdate {
							continue
						}
					}
					var ectx *tree.EvalContext
					cmp := lval.Compare(ectx, rval)
					if Boolfromcmp(op, cmp) {
						err := Call(ctx, txn, params, tg.Tgargs, int(tg.TgfuncID), tg.Tgname, tg.Tgrelid)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil

}

//Call to call
func Call(
	ctx context.Context,
	txn *client.Txn,
	params runParams,
	funcArgs []string,
	funcID int,
	triggerName string,
	idOfTable tree.ID,
) error {
	funcDesc, err := sqlbase.GetFunctionDescFromID(ctx, txn, sqlbase.ID(funcID))
	if err != nil {
		return fmt.Errorf("procedure for trigger %s does not exist", triggerName)
	}
	// 需要获取该function所属的db和schema
	var funcName string
	if schDesc, err := getSchemaDescByID(ctx, txn, funcDesc.ParentID); err == nil {
		var db, sch string
		sch = schDesc.Name
		if dbDesc, err := getDatabaseDescByID(ctx, txn, schDesc.ParentID); err == nil {
			db = dbDesc.Name
			funcName = db + "." + sch + "." + funcDesc.Name
		} else {
			return fmt.Errorf("procedure for trigger %s does not exist", triggerName)
		}
	} else {
		return fmt.Errorf("procedure for trigger %s does not exist", triggerName)
	}
	// 获取该table的name,db和schema等信息
	var tableName tree.TableName
	tabDesc, err := sqlbase.GetTableDescFromID(ctx, txn, sqlbase.ID(idOfTable))
	if err != nil {
		return fmt.Errorf("table of this trigger does not exist, please try again")
	}
	tableName.TableName = tree.Name(tabDesc.Name)
	if schDesc, err := getSchemaDescByID(ctx, txn, tabDesc.ParentID); err == nil {
		tableName.SchemaName = tree.Name(schDesc.Name)
		if dbDesc, err := getDatabaseDescByID(ctx, txn, schDesc.ParentID); err == nil {
			tableName.CatalogName = tree.Name(dbDesc.Name)
			tableName.ExplicitCatalog = true
			tableName.ExplicitSchema = true
		} else {
			return fmt.Errorf("table of this trigger does not exist, please try again")
		}
	} else {
		return fmt.Errorf("table of this trigger does not exist, please try again")
	}
	var sqlQuery = "call %s("
	sqlQuery = fmt.Sprintf(sqlQuery, funcName)
	for k, value := range funcArgs {
		if k != 0 {
			sqlQuery += ","
		}
		sqlQuery += value
		// type convert
		typeCon := fmt.Sprintf("::%s", funcDesc.Args[k].Type.VisibleTypeName)
		sqlQuery += typeCon
	}
	sqlQuery += ")"

	_, err = params.p.extendedEvalCtx.InternalExecutor.(*SessionBoundInternalExecutor).Exec(ctx, "call", txn, sqlQuery)
	if err != nil {
		return err
	}
	return nil
}

func getValue(
	expr tree.Expr, tabledesc *sqlbase.ImmutableTableDescriptor, sourceVals tree.Datums,
) (star bool, value tree.Datum, err error) {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		if t.Star {
			return true, nil, nil
		}
		for col := range tabledesc.Columns {
			if tabledesc.Columns[col].Name == t.Parts[0] {
				return false, sourceVals[col], nil
			}
		}
	default:
		var ctx *tree.SemaContext
		var ectx *tree.EvalContext
		texpr, err := tree.TypeCheck(t, ctx, types.Any, false)
		if err != nil {
			return false, nil, err
		}
		val, err := texpr.Eval(ectx)
		if err != nil {
			return false, nil, err
		}
		return false, val, nil
	}
	return false, nil, nil
}

//Boolfromcmp from comparison
func Boolfromcmp(op tree.ComparisonOperator, cmp int) bool {
	switch op {
	case tree.EQ, tree.IsNotDistinctFrom:
		return cmp == 0
	case tree.LT:
		return cmp < 0
	case tree.LE:
		return cmp <= 0
	case tree.GT:
		return cmp > 0
	case tree.GE:
		return cmp >= 0
	case tree.IsDistinctFrom, tree.NE:
		return cmp != 0
	default:
		panic(fmt.Sprintf("unexpected ComparisonOperator in boolFromCmp: %v", op))
	}
}
