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
	"strconv"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security/audit/event/infos"
	"github.com/znbasedb/znbase/pkg/security/audit/server"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgcode"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/schemaexpr"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

type createIndexNode struct {
	n         *tree.CreateIndex
	tableDesc *sqlbase.MutableTableDescriptor
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *tree.CreateIndex) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, requireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}

	if tableDesc.IsView() && !tableDesc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
	}

	if tableDesc.MaterializedView() {
		if n.Interleave != nil {
			return nil, pgerror.NewError(pgcode.InvalidObjectDefinition,
				"cannot create interleaved index on materialized view")
		}
		if n.Unique {
			return nil, pgerror.NewError(pgcode.InvalidObjectDefinition,
				"cannot create unique index on materialized view")
		}
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.REFERENCES); err != nil {
		return nil, err
	}

	return &createIndexNode{tableDesc: tableDesc, n: n}, nil
}

// MakeIndexDescriptor creates an index descriptor from a CreateIndex node.
func MakeIndexDescriptor(params runParams, n *createIndexNode) (*sqlbase.IndexDescriptor, error) {
	indexDesc := sqlbase.IndexDescriptor{
		Name:             string(n.n.Name),
		Unique:           n.n.Unique,
		StoreColumnNames: n.n.Storing.ToStrings(),
		IsLocal:          n.n.IsLocal,
	}

	if n.n.Inverted {
		if n.n.Interleave != nil {
			return nil, pgerror.NewError(pgcode.InvalidSQLStatementName, "inverted indexes don't support interleaved tables")
		}

		if n.n.PartitionBy != nil {
			return nil, pgerror.NewError(pgcode.InvalidSQLStatementName, "inverted indexes don't support partitioning")
		}

		if len(indexDesc.StoreColumnNames) > 0 {
			return nil, pgerror.NewError(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns")
		}

		if n.n.Unique {
			return nil, pgerror.NewError(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique")
		}
		indexDesc.Type = sqlbase.IndexDescriptor_INVERTED
	}

	var funcflag bool
	for _, c := range n.n.Columns {
		if c.IsFuncIndex() {
			funcflag = true
		}
	}

	if n.n.Where != nil && funcflag {
		return nil, pgerror.NewError(pgcode.InvalidSQLStatementName, "function and partial indexes can't be supported")
	}

	if n.n.Where != nil {
		// TODO(mgartner): remove this once partial indexes are fully supported.

		idxValidator := schemaexpr.MakeIndexPredicateValidator(params.ctx, n.n.Table, n.tableDesc.TableDescriptor, &params.p.semaCtx)
		_, err := idxValidator.Validate(n.n.Where.Expr)
		if err != nil {
			return nil, err
		}
		indexDesc.PredExpr = tree.Serialize(n.n.Where.Expr)
	}

	var isFunc bool
	for _, c := range n.n.Columns {
		if c.IsFuncIndex() {
			semaContext := tree.MakeSemaContext()
			semaContext.Properties.Require("CREATE FUNCTION INDEX", 26)
			semactx := &semaContext
			var searchPath sessiondata.SearchPath
			searchPath = semaContext.SearchPath
			funcexpr, ok := c.Function.(*tree.FuncExpr)
			if !ok {
				return nil, errors.Errorf("Non-function index is not allowed")
			}
			def, err := funcexpr.Func.Resolve(searchPath, false)
			if err != nil {
				return nil, err
			}

			if err := semactx.CheckFunctionUsage(funcexpr, def); err != nil {
				return nil, errors.Wrapf(err, "%s()", def.Name)
			}
			for _, expr := range funcexpr.Exprs {
				if _, ok := expr.(*tree.FuncExpr); ok {
					return nil, errors.Errorf("Nested-function index is not allowed")
				}
			}
			isFunc = true
		}
	}

	if isFunc {
		indexDesc.FillFuncColumns(n.tableDesc)
		if err := indexDesc.FillFunctionColumns(n.n.Columns); err != nil {
			return nil, err
		}
	} else {
		if err := indexDesc.FillColumns(n.n.Columns); err != nil {
			return nil, err
		}
	}

	if n.n.LocateSpaceName != nil {
		if len(n.n.LocateSpaceName.Locate) != 0 {
			tempLocate := &roachpb.LocationValue{}
			tempLocate.Spaces = make([]string, len(n.n.LocateSpaceName.Locate))
			if n.n.LocateSpaceName.Lease != nil {
				tempLocate.Leases = make([]string, len(n.n.LocateSpaceName.Lease))
			}
			for i, v := range n.n.LocateSpaceName.Locate {
				tempLocate.Spaces[i] = string(v)
				indexDesc.LocationNums++
			}
			for i, v := range n.n.LocateSpaceName.Lease {
				tempLocate.Leases[i] = string(v)
			}
			indexDesc.LocateSpaceName = tempLocate
		}
	}
	return &indexDesc, nil
}

func (n *createIndexNode) startExec(params runParams) error {
	_, dropped, err := n.tableDesc.FindIndexByName(string(n.n.Name))
	if err == nil {
		if dropped {
			return fmt.Errorf("index %q being dropped, try again later", string(n.n.Name))
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	if n.n.IsLocal && n.tableDesc.PrimaryIndex.Partitioning.IsHashPartition {
		// hash分区实际上以hashnum列作为分区列, 为保证本地分区索引和表分区hash分区一致, 在用户索引列没有手动添加该列时,需要自动添加
		flag := false
		for _, name := range n.n.Columns {
			if name.Column == "hashnum" {
				flag = true
			}
		}
		if !flag {
			tmpColumns := n.n.Columns
			n.n.Columns = make([]tree.IndexElem, 0)
			n.n.Columns = append(n.n.Columns, tree.IndexElem{Column: "hashnum"})
			n.n.Columns = append(n.n.Columns, tmpColumns...)
		}
	}

	indexDesc, err := MakeIndexDescriptor(params, n)
	if err != nil {
		return err
	}
	n.tableDesc.LocationNums += indexDesc.LocationNums

	// geo-partition feature add by jiye
	// first check locate space valid or not
	spaceName := n.n.LocateSpaceName.ToValue()
	if spaceName != nil {
		if err := CheckLocateSpaceNameExistICL(params.ctx,
			spaceName, *params.StatusServer()); err != nil {
			return err
		}
		// update system.location with because index with locate in attribute
		if err := params.p.LocationMapChange(params.ctx, n.tableDesc,
			params.extendedEvalCtx.Tables.databaseCache.systemConfig); err != nil {
			return err
		}
	}
	// update locationMap index locate
	indexDesc.LocateSpaceName = n.n.LocateSpaceName.ToValue()
	if n.n.PartitionBy != nil {
		if n.n.PartitionBy.IsHash {
			return pgerror.NewError(pgcode.IndexHashPartition, "Can not append hash partition to index.")
		}
		partitioning, err := CreatePartitioning(params.ctx, params.p.ExecCfg().Settings,
			params.EvalContext(), n.tableDesc, indexDesc, n.n.PartitionBy, params.StatusServer())
		if err != nil {
			return err
		}
		indexDesc.Partitioning = partitioning
	}
	// Deal with local index, create partitioning from Table partitioning
	if indexDesc.IsLocal {
		columnNameList := make([]string, 0)
		for _, k := range n.n.Columns {
			columnNameList = append(columnNameList, string(k.Column))
		}
		Partitioning, err := createLocalIndexPartitioning(n.tableDesc, n.tableDesc.PrimaryIndex, string(n.n.Name), columnNameList, n.n.LocalIndexPartitionName, n.n.Unique)
		if err != nil {
			return err
		}
		indexDesc.Partitioning = Partitioning
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
		return err
	}

	// The index name may have changed as a result of
	// AllocateIDs(). Retrieve it for the event log below.
	index := n.tableDesc.Mutations[mutationIdx].GetIndex()
	indexName := index.Name

	if n.n.Interleave != nil {
		if err := params.p.addInterleave(params.ctx, n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := params.p.finalizeInterleave(params.ctx, n.tableDesc, index); err != nil {
			return err
		}
	}

	mutationID, err := params.p.createOrUpdateSchemaChangeJob(params.ctx, n.tableDesc,
		tree.AsStringWithFlags(n.n, tree.FmtAlwaysQualifyTableNames|tree.FmtVisableType))
	if err != nil {
		return err
	}
	if err := params.p.writeSchemaChange(params.ctx, n.tableDesc, mutationID); err != nil {
		return err
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	params.p.curPlan.auditInfo = &server.AuditInfo{
		EventTime: timeutil.Now(),
		EventType: string(EventLogCreateIndex),
		TargetInfo: &server.TargetInfo{
			TargetID: int32(n.tableDesc.ID),
			Desc: struct {
				User      string
				TableName string
				IndexName string
			}{
				params.SessionData().User,
				n.n.Table.FQString(),
				indexName,
			},
		},
		Info: &infos.CreateIndexInfo{
			TableName: n.n.Table.FQString(), IndexName: indexName, Statement: n.n.String(),
			User: params.SessionData().User, MutationID: uint32(mutationID),
		},
	}
	return nil
}

func (*createIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*createIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*createIndexNode) Close(context.Context)        {}

// createLocalIndexPartitioning create partitioning descriptor for local-partition-index
// 用于为本地分区索引创建分区部分的描述符的函数
func createLocalIndexPartitioning(
	TableDesc *sqlbase.MutableTableDescriptor, // 表的描述符
	PrimaryIndex sqlbase.IndexDescriptor, // 表的主键索引
	LocalIndexName string, // 本地分区索引索引名
	LocalIndexColumns []string, // 本地分区索引的索引列列表
	LocalIndexPartitionName []string, // 本地分区索引自定义分区命名
	IsUnique bool, // 本地分区索引是否为唯一索引
) (sqlbase.PartitioningDescriptor, error) {

	var partitionDesc sqlbase.PartitioningDescriptor
	var errOfDeepCopy error // 进行分区深度拷贝时发生的错误
	prefix := false         // 标记表的分区列是否为本地分区索引的前缀
	indexTP := PrimaryIndex.Partitioning
	IsHash := indexTP.IsHashPartition
	lenOfDefineName := len(LocalIndexPartitionName) // 用户自定义索引分区命名
	hasDefineName := lenOfDefineName != 0           // 标记是否存在自定义命名
	numsForRange, numsForList := 0, 0               // 用于为本地分区索引命名
	// 验证是否所有的列均存在于表中
	for _, indexColumn := range LocalIndexColumns {
		for i, v := range TableDesc.Columns {
			if v.Name == indexColumn {
				break
			}
			if i == len(TableDesc.Columns)-1 {
				return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate,
					fmt.Sprintf("index \"%s\" contains unknown column \"%s\"", LocalIndexName, indexColumn))
			}
		}
	}
	// only partition table can create local-partition-index
	// 非分区表无法创建本地分区索引
	if indexTP.List == nil && indexTP.Range == nil {
		return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate, "Underlying table of a LOCAL partitioned index must be partitioned")
	}
	// make descriptor of local-partition-index from table partitionDesc, and rename partition names if user
	// defined the names of partition. Mind that list partition may have subpartition.
	// 依据分区表的分区信息构建本地分区索引的分区描述符
	// 如果用户有自定义名称, 则需要对分区进行重命名。否则命名规则为 索引名 + "_" + 分区序列号 + "_local_range" / "_local_list"
	// 注 : list分区可以拥有子分区, range不能
	var deepCopyFromPartition func(p sqlbase.PartitioningDescriptor) (sqlbase.PartitioningDescriptor, error)
	deepCopyFromPartition = func(p sqlbase.PartitioningDescriptor) (sqlbase.PartitioningDescriptor, error) {
		var newPartition sqlbase.PartitioningDescriptor
		var err error
		newPartition.NumColumns = p.NumColumns //该赋值不适用于最外层分区, 对于该赋值需要进行额外的判断
		newPartition.IsHashPartition = p.IsHashPartition
		// make range partition
		if p.Range != nil {
			newPartition.Range = make([]sqlbase.PartitioningDescriptor_Range, len(p.Range))
			copy(newPartition.Range, p.Range)
			for i := range newPartition.Range {
				if hasDefineName {
					index := numsForRange + numsForList
					if index < lenOfDefineName {
						newPartition.Range[i].Name = LocalIndexPartitionName[index]
					} else {
						return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate, "number of partitions of LOCAL index must equal that of the underlying table")
					}
				} else {
					newPartition.Range[i].Name = LocalIndexName + "_" + strconv.Itoa(numsForRange) + "_local_range"
				}
				numsForRange++
			}
		}
		// make list partition and its subpartition
		if p.List != nil {
			newPartition.List = make([]sqlbase.PartitioningDescriptor_List, len(p.List))
			copy(newPartition.List, p.List)
			for i := range newPartition.List {
				if hasDefineName {
					index := numsForRange + numsForList
					if index < lenOfDefineName {
						newPartition.List[i].Name = LocalIndexPartitionName[index]
					} else {
						return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate, "number of partitions of LOCAL index must equal that of the underlying table")
					}
				} else {
					newPartition.List[i].Name = LocalIndexName + "_" + strconv.Itoa(numsForList) + "_local_list"
				}
				numsForList++
				newPartition.List[i].Subpartitioning, err = deepCopyFromPartition(newPartition.List[i].Subpartitioning)
				if err != nil {
					return sqlbase.PartitioningDescriptor{}, err
				}
			}
		}
		return newPartition, nil
	}
	partitionDesc, errOfDeepCopy = deepCopyFromPartition(indexTP)
	if errOfDeepCopy != nil {
		return sqlbase.PartitioningDescriptor{}, errOfDeepCopy
	}
	// 对自定义分区的: 1.数目 2.命名合法性 进行判断.
	if hasDefineName {
		// 再次判断自定义命名的个数是否与分区个数相符, 函数deepCopyFromPartition仅能处理自定义命名个数不足的情况
		if lenOfDefineName != numsForList+numsForRange {
			return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate, "number of partitions of LOCAL index must equal that of the underlying table")
		}
		// 由于符合以 _local_list, _local_range结尾进行命名的索引只允许在local-index自动命名中使用,因此需要检查合法性
		for _, name := range LocalIndexPartitionName {
			if err := isAutoNameForLocalIndexPartitioning(name); err != nil {
				return sqlbase.PartitioningDescriptor{}, err
			}
		}
	}
	// check the NumColumns of index partition Desc
	// 本地分区索引存在最大的限制是, 表的分区键必须是该索引的前缀. 由于表的subpartition和索引遵循相同的规则,
	// 因此只需要检查前n列是否满足条件. n表示表分区列
	// 当类型不一致时, 该赋值会导致panic; NumColumns的值为N表示前N列为（该层）索引分区列, 0表示该索引不分区
	tablePartitionNum := 0
	partitionDesc.NumColumns = 0 // avoid panic, 0 means no partition
	for pl := indexTP; pl.NumColumns != 0; {
		tablePartitionNum += int(pl.NumColumns)
		if pl.List == nil {
			break
		}
		pl = pl.List[0].Subpartitioning
	}
	// 如果是hash分区, 需要额外往后检查一列保证符合前缀的要求
	if IsHash && tablePartitionNum < len(LocalIndexName) {
		tablePartitionNum++
	}
	if len(LocalIndexColumns) >= tablePartitionNum {
		count := 0
		for i := 0; i < tablePartitionNum; i++ {
			if PrimaryIndex.ColumnNames[i] != LocalIndexColumns[i] {
				break
			}
			count++
		}
		if count == tablePartitionNum {
			partitionDesc.NumColumns = indexTP.NumColumns
			prefix = true
		}
	}
	if !prefix {
		if IsUnique {
			return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate, "Table partitioning columns must form a prefix of key columns of a UNIQUE index.")
		}
		if IsHash {
			return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate,
				fmt.Sprintf("do not support non-prefixed local partition index temporarily."))
		}
		return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexCreate,
			fmt.Sprintf("do not support non-prefixed local partition index temporarily.\nfirst %d columns in index should match table partition columns %s",
				tablePartitionNum,
				PrimaryIndex.ColumnNames[:tablePartitionNum]))
	}
	// finally return the desc of partitioning
	return partitionDesc, nil
}

// alterLocalIndexPartitioning alter local-index's partitioning descriptor when alter table's partitioning
// 用于表分区被修改时,修改本地分区索引分区的函数
func alterLocalIndexPartitioning(
	TableDesc *sqlbase.MutableTableDescriptor,
	PrimaryIndex sqlbase.IndexDescriptor, // 表的主键索引
	LocalIndex sqlbase.IndexDescriptor, // 需要调整分区信息的本地分区索引描述符
) (sqlbase.PartitioningDescriptor, error) {

	tablePartitionColumns := make([]string, 0) // 表的分区列
	localIndexColumns := LocalIndex.ColumnNames
	suitablePartitionCol := true // 标记表的新分区是否满足调整本地分区索引的需求
	colsOfTablePartition := 0    // 代表主键列的前n列为表分区列
	// calculate cols of table partition
	for p := PrimaryIndex.Partitioning; p.NumColumns != 0; {
		colsOfTablePartition += int(p.NumColumns)
		if p.List == nil {
			break
		}
		p = p.List[0].Subpartitioning
	}
	for i := 0; i < colsOfTablePartition; i++ {
		tablePartitionColumns = append(tablePartitionColumns, PrimaryIndex.ColumnNames[i])
	}
	// 新的表分区列需要满足以下条件 : 新的分区列需要是本地分区索引索引列的前缀
	if len(tablePartitionColumns) <= len(localIndexColumns) {
		for i := 0; i < len(tablePartitionColumns); i++ {
			if tablePartitionColumns[i] != localIndexColumns[i] {
				suitablePartitionCol = false
				break
			}
		}
	} else {
		suitablePartitionCol = false
	}
	if !suitablePartitionCol {
		return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexAlter,
			fmt.Sprintf("can not alter this table's partition because local-index (%s) can't be adjust Automatically to follow this change of table partition list.\nyou can drop and create this table or drop this index first",
				LocalIndex.Name))
	}
	// 重新为索引构建分区信息
	partitioning, err := createLocalIndexPartitioning(TableDesc,
		PrimaryIndex,
		LocalIndex.Name,
		localIndexColumns,
		nil,
		LocalIndex.Unique)
	if err != nil {
		return sqlbase.PartitioningDescriptor{}, pgerror.NewError(pgcode.LocalIndexAlter,
			fmt.Sprintf("alter partition for local-index '%s' automatically failed, you can  drop this index first", LocalIndex.Name))
	}
	return partitioning, nil
}

var (
	autoNameFormat1 = "_local_list"
	autoNameFormat2 = "_local_range"
	lenAutoName1    = len(autoNameFormat1)
	lenAutoName2    = len(autoNameFormat2)
	errAutoName1    = fmt.Errorf(fmt.Sprintf("partition name is invalid which end with '%s' because it was only used in automatic naming for local-index", autoNameFormat1))
	errAutoName2    = fmt.Errorf(fmt.Sprintf("partition name is invalid which end with '%s' because it was only used in automatic naming for local-index", autoNameFormat2))
)

// isAutoNameForLocalIndexPartitioning 用于检测一个分区命名是否只允许在本地分区索引自动命名时使用
func isAutoNameForLocalIndexPartitioning(name string) error {
	if len(name) >= lenAutoName1 && name[len(name)-lenAutoName1:] == autoNameFormat1 {
		return errAutoName1
	}
	if len(name) >= lenAutoName2 && name[len(name)-lenAutoName2:] == autoNameFormat2 {
		return errAutoName2
	}
	return nil
}
