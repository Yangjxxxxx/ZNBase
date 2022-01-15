package changefeedicl

import (
	"context"

	"github.com/lib/pq/oid"
	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/protoutil"
)

// DDLEvents DDL变更消息
type DDLEvents []DDLEvent

// DDLEvent DDL变更消息
type DDLEvent struct {
	Schema                      string                                `json:",omitempty"`
	TableName                   string                                `json:",omitempty"`
	TableID                     sqlbase.ID                            `json:",omitempty"`
	BeforeTableName             string                                `json:",omitempty"`
	Column                      map[string]Column                     `json:",omitempty"`
	RenameColumn                *renameColumn                         `json:",omitempty"`
	ModifyColumn                *modifyColumn                         `json:",omitempty"`
	SetDefaultValue             *setDefaultValue                      `json:",omitempty"`
	RenameIndexName             *renameIndex                          `json:",omitempty"`
	ForeignKeyReference         *sqlbase.ForeignKeyReference          `json:",omitempty"`
	PartitioningDescriptor      *sqlbase.PartitioningDescriptor       `json:",omitempty"`
	TableDescriptorSequenceOpts *sqlbase.TableDescriptor_SequenceOpts `json:",omitempty"`
	ViewQuery                   string                                `json:",omitempty"`
	Index                       map[string]*sqlbase.IndexDescriptor   `json:",omitempty"`
	Operate                     TableOperate                          `json:",omitempty"`
	Ts                          string                                `json:",omitempty"`
	eventAll                    int
	eventSend                   int
}

func getBasicEvent(
	ctx context.Context, tblDesc, preTblDesc *sqlbase.TableDescriptor, DB *client.DB,
) (event DDLEvent, err error) {
	schema, err := sqlbase.GetSchemaDescFromID(ctx, DB.NewTxn(ctx, "cdc-ddl-findSchemaName"), tblDesc.ParentID)
	if err != nil {
		return event, err
	}
	event = DDLEvent{
		TableName: tblDesc.GetName(),
		Schema:    schema.Name,
		TableID:   tblDesc.ID,
	}
	return event, err
}

//IsEmpty 判断DDL message 是否为空
func (d *DDLEvent) IsEmpty() bool {
	return d.Schema == "" && d.TableName == "" && d.TableID == 0 && d.BeforeTableName == "" && len(d.Column) == 0 && len(d.Index) == 0 && d.Operate == UnknownOperation
}

// Column DDL 变更列信息
type Column struct {
	ID          oid.Oid
	Nullable    bool
	DefaultExpr *string
}

func getColumn(column sqlbase.ColumnDescriptor) Column {
	return Column{
		ID:          getPGOidFromColumnType(column),
		Nullable:    column.Nullable,
		DefaultExpr: column.DefaultExpr,
	}
}

type modifyColumn struct {
	ColumnName     string
	ID             oid.Oid
	OldID          oid.Oid
	VisibleType    string
	OldVisibleType string
}

func getModifyColumn(colDesc, preColDesc sqlbase.ColumnDescriptor) *modifyColumn {
	return &modifyColumn{
		ColumnName:     colDesc.Name,
		ID:             getPGOidFromColumnType(colDesc),
		OldID:          getPGOidFromColumnType(preColDesc),
		VisibleType:    colDesc.VisibleType(),
		OldVisibleType: preColDesc.VisibleType(),
	}
}

type setDefaultValue struct {
	ColumnName      string
	ID              oid.Oid
	DefaultValue    *string
	OldDefaultValue *string
}

func getSetDefaultValue(colDesc, preColDesc sqlbase.ColumnDescriptor) *setDefaultValue {
	return &setDefaultValue{
		ColumnName:      colDesc.Name,
		ID:              getPGOidFromColumnType(colDesc),
		DefaultValue:    colDesc.DefaultExpr,
		OldDefaultValue: preColDesc.DefaultExpr,
	}
}

type renameIndex struct {
	IndexName    string
	OldIndexName string
}

func getRenameIndex(idxDesc, preIdxDesc sqlbase.IndexDescriptor) *renameIndex {
	return &renameIndex{
		IndexName:    idxDesc.Name,
		OldIndexName: preIdxDesc.Name,
	}
}

type renameColumn struct {
	ColumnName    string
	OldColumnName string
}

func getRenameColumn(colDesc, preColDesc sqlbase.ColumnDescriptor) *renameColumn {
	return &renameColumn{
		ColumnName:    colDesc.Name,
		OldColumnName: preColDesc.Name,
	}
}

func getDDLEventFromTableDescriptor(
	ctx context.Context,
	desc, preDesc *sqlbase.Descriptor,
	DB *client.DB,
	descTs, preDescTs hlc.Timestamp,
) (events []DDLEvent, err error) {
	events = make([]DDLEvent, 0)
	//TODO:schema change capture
	//schema change
	//if desc != nil && preDesc == nil && desc.GetSchema() != nil {
	//	event := DDLEvent{}
	//	event.Operate = CreateSchema
	//	events = append(events, event)
	//	return events, err
	//}
	//if desc != nil && desc.GetSchema() == nil {
	//	event := DDLEvent{}
	//	event.Operate = DropSchema
	//	events = append(events, event)
	//	return events, err
	//}
	//table change
	if desc.Table(descTs) != nil {
		//database level change
		tblDesc, preTblDesc := desc.Table(descTs), preDesc.Table(preDescTs)
		if preTblDesc == nil && tblDesc != nil && tblDesc.IsSequence() {
			event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
			if err != nil {
				return nil, err
			}
			event.TableDescriptorSequenceOpts = tblDesc.SequenceOpts
			event.Operate = CreateSequence
			events = append(events, event)
			return events, err
		}
		if tblDesc != nil && tblDesc.State == sqlbase.TableDescriptor_DROP && tblDesc.DrainingNames == nil && tblDesc.IsSequence() {
			event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
			if err != nil {
				return nil, err
			}
			event.Operate = DropSequence
			events = append(events, event)
			return events, err
		}
		if preTblDesc == nil && tblDesc != nil && tblDesc.IsView() {
			columnsMap := make(map[string]Column)
			event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
			if err != nil {
				return nil, err
			}
			for _, col := range tblDesc.Columns {
				columnsMap[col.Name] = getColumn(col)
			}
			event.ViewQuery = tblDesc.ViewQuery
			event.Column = columnsMap
			event.Operate = CreateView
			events = append(events, event)
			return events, err
		}
		if tblDesc != nil && tblDesc.State == sqlbase.TableDescriptor_DROP && tblDesc.DrainingNames == nil && tblDesc.IsView() {
			event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
			if err != nil {
				return nil, err
			}
			event.Operate = DropView
			events = append(events, event)
			return events, err
		}
		if preTblDesc == nil && tblDesc != nil {
			columnsMap := make(map[string]Column)
			event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
			if err != nil {
				return nil, err
			}
			for _, col := range tblDesc.Columns {
				columnsMap[col.Name] = getColumn(col)
			}
			indexMap := make(map[string]*sqlbase.IndexDescriptor)
			for _, descIndex := range tblDesc.Indexes {
				indexMap[descIndex.Name] = &descIndex
			}
			event.Column = columnsMap
			event.Index = indexMap
			event.Operate = CreateTable
			events = append(events, event)
			return events, err
		}
		if tblDesc != nil && tblDesc.DrainingNames != nil {
			if len(tblDesc.DrainingNames) != 0 {
				event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
				if err != nil {
					return nil, err
				}
				event.BeforeTableName = tblDesc.DrainingNames[0].Name
				event.Operate = RenameTable
				events = append(events, event)
				return events, err
			}
		}
		if tblDesc != nil && tblDesc.State == sqlbase.TableDescriptor_DROP && tblDesc.DrainingNames == nil {
			event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
			if err != nil {
				return nil, err
			}
			event.Operate = DropTable
			events = append(events, event)
			return events, err
		}

		//table level change
		//get event from mutations
		if tblDesc != nil && len(tblDesc.Mutations) != 0 {
			for _, mutation := range tblDesc.Mutations {
				columnsMap := make(map[string]Column)
				event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
				if err != nil {
					return nil, err
				}
				switch mutation.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					if mutation.GetColumn() != nil {
						columnsMap[mutation.GetColumn().Name] = getColumn(*mutation.GetColumn())
					}
					event.Column = columnsMap
					if mutation.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY && mutation.Direction == sqlbase.DescriptorMutation_ADD {
						event.Operate = AddColumn
						events = append(events, event)
					}
					if mutation.State == sqlbase.DescriptorMutation_DELETE_ONLY && mutation.Direction == sqlbase.DescriptorMutation_DROP {
						event.Operate = DropColumn
						events = append(events, event)
					}
				case *sqlbase.DescriptorMutation_Index:
					indexMap := make(map[string]*sqlbase.IndexDescriptor)
					indexMap[mutation.GetIndex().Name] = mutation.GetIndex()
					event.Index = indexMap
					if mutation.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY && mutation.Direction == sqlbase.DescriptorMutation_ADD {
						event.Operate = AddIndex
						events = append(events, event)
					}
					if mutation.State == sqlbase.DescriptorMutation_DELETE_ONLY && mutation.Direction == sqlbase.DescriptorMutation_DROP {
						event.Operate = DropIndex
						events = append(events, event)
					}
				}
			}
		}

		//get event from indexes
		if tblDesc != nil && preTblDesc != nil && len(tblDesc.Indexes) == len(preTblDesc.Indexes) {
			for i := 0; i < len(tblDesc.Indexes); i++ {
				if tblDesc.Indexes[i].Name != preTblDesc.Indexes[i].Name {
					event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
					if err != nil {
						return nil, err
					}
					event.RenameIndexName = getRenameIndex(tblDesc.Indexes[i], preTblDesc.Indexes[i])
					event.Operate = RenameIndex
					events = append(events, event)
				}
				if !tblDesc.Indexes[i].ForeignKey.Equal(sqlbase.ForeignKeyReference{}) && preTblDesc.Indexes[i].ForeignKey.Equal(sqlbase.ForeignKeyReference{}) {
					event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
					if err != nil {
						return nil, err
					}
					event.ForeignKeyReference = &tblDesc.Indexes[i].ForeignKey
					event.Operate = AddForeignKey
					events = append(events, event)
				}
				if tblDesc.Indexes[i].ForeignKey.Equal(sqlbase.ForeignKeyReference{}) && !preTblDesc.Indexes[i].ForeignKey.Equal(sqlbase.ForeignKeyReference{}) {
					event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
					if err != nil {
						return nil, err
					}
					event.ForeignKeyReference = &preTblDesc.Indexes[i].ForeignKey
					event.Operate = DropForeignKey
					events = append(events, event)
				}
			}
		}

		//get event from columns
		if tblDesc != nil && preTblDesc != nil && len(tblDesc.Columns) == len(preTblDesc.Columns) {
			for i := 0; i < len(tblDesc.Columns); i++ {
				if tblDesc.Columns[i].VisibleType() != preTblDesc.Columns[i].VisibleType() {
					event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
					if err != nil {
						return nil, err
					}
					event.ModifyColumn = getModifyColumn(tblDesc.Columns[i], preTblDesc.Columns[i])
					event.Operate = ModifyColumn
					events = append(events, event)
				}
				var defaultValueChange bool
				if tblDesc.Columns[i].DefaultExpr != nil && preTblDesc.Columns[i].DefaultExpr == nil {
					defaultValueChange = true
				}
				if tblDesc.Columns[i].DefaultExpr == nil && preTblDesc.Columns[i].DefaultExpr != nil {
					defaultValueChange = true
				}
				if tblDesc.Columns[i].DefaultExpr != nil && preTblDesc.Columns[i].DefaultExpr != nil && *tblDesc.Columns[i].DefaultExpr != *preTblDesc.Columns[i].DefaultExpr {
					defaultValueChange = true
				}
				if defaultValueChange {
					event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
					if err != nil {
						return nil, err
					}
					event.SetDefaultValue = getSetDefaultValue(tblDesc.Columns[i], preTblDesc.Columns[i])
					event.Operate = SetDefaultValue
					events = append(events, event)
				}
				if tblDesc.Columns[i].Name != "" && preTblDesc.Columns[i].Name != "" && tblDesc.Columns[i].Name != preTblDesc.Columns[i].Name {
					event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
					if err != nil {
						return nil, err
					}
					event.RenameColumn = getRenameColumn(tblDesc.Columns[i], preTblDesc.Columns[i])
					event.Operate = RenameColumn
					events = append(events, event)
				}
			}
		}

		//get event from primaryIndex
		if tblDesc != nil && preTblDesc != nil {
			partition, prePartition := tblDesc.PrimaryIndex.Partitioning, preTblDesc.PrimaryIndex.Partitioning
			emptyPartition := sqlbase.PartitioningDescriptor{}
			if !partition.Equal(emptyPartition) && prePartition.Equal(emptyPartition) {
				event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
				if err != nil {
					return nil, err
				}
				event.PartitioningDescriptor = &partition
				event.Operate = AddTablePartition
				events = append(events, event)
			}
			if partition.Equal(emptyPartition) && !prePartition.Equal(emptyPartition) {
				event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
				if err != nil {
					return nil, err
				}
				event.PartitioningDescriptor = &prePartition
				event.Operate = DropTablePartition
				events = append(events, event)
			}
			fk, preFk := tblDesc.PrimaryIndex.ForeignKey, preTblDesc.PrimaryIndex.ForeignKey
			emptyFk := sqlbase.ForeignKeyReference{}
			if fk != emptyFk && preFk == emptyFk {
				event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
				if err != nil {
					return nil, err
				}
				event.ForeignKeyReference = &fk
				event.Operate = AddForeignKey
				events = append(events, event)
			}
			if fk == emptyFk && preFk != emptyFk {
				event, err := getBasicEvent(ctx, tblDesc, preTblDesc, DB)
				if err != nil {
					return nil, err
				}
				event.ForeignKeyReference = &preFk
				event.Operate = DropForeignKey
				events = append(events, event)
			}
		}
		return events, err
	}
	return nil, err
}

// TableOperate DDL 操作映射
type TableOperate int

const (
	// UnknownOperation 未知操作
	UnknownOperation TableOperate = 0
	//// CreateSchema 创建模式
	//CreateSchema           TableOperate = 1
	//DropSchema             TableOperate = 2

	// CreateTable 创建表操作
	CreateTable TableOperate = 3
	// DropTable 删除表操作
	DropTable TableOperate = 4
	// AddColumn 加列操作
	AddColumn TableOperate = 5
	// DropColumn 删除列操作
	DropColumn TableOperate = 6
	// AddIndex 增加索引操作
	AddIndex TableOperate = 7
	// DropIndex 删除索引操作
	DropIndex TableOperate = 8
	// AddForeignKey 增加外键操作
	AddForeignKey TableOperate = 9
	// DropForeignKey 删除外键操作
	DropForeignKey TableOperate = 10
	//TruncateTable          TableOperate = 11

	// ModifyColumn 修改列类型操作
	ModifyColumn TableOperate = 12
	//RebaseAutoID           TableOperate = 13

	// RenameTable 修改表名操作
	RenameTable TableOperate = 14
	// SetDefaultValue 设置默认值操作
	SetDefaultValue TableOperate = 15
	//ModifyTableComment     TableOperate = 16

	// RenameIndex 修改索引名操作
	RenameIndex TableOperate = 17
	// AddTablePartition 增加表分区操作
	AddTablePartition TableOperate = 18
	// DropTablePartition 删除表分区操作
	DropTablePartition TableOperate = 19
	// CreateView 创建视图操作
	CreateView TableOperate = 20
	//TruncateTablePartition TableOperate = 21

	// DropView 删除视图操作
	DropView TableOperate = 22
	//AddPrimaryKey          TableOperate = 23
	//DropPrimaryKey         TableOperate = 24

	// CreateSequence 创建序列操作
	CreateSequence TableOperate = 25
	//AlterSequence          TableOperate = 26

	// DropSequence 删除序列操作
	DropSequence TableOperate = 27
	// RenameColumn 修改列名操作
	RenameColumn TableOperate = 28
)

// NeedContinue 判断当前DDL event 是否需要获取下一次触发时间
func (o TableOperate) NeedContinue() bool {
	switch o {
	case AddColumn, DropColumn, AddIndex, DropIndex:
		return true
	default:
		return false
	}
}

func parseDescriptor(
	col sqlbase.ColumnDescriptor, datum tree.Datum, descTs hlc.Timestamp,
) (*sqlbase.TableDescriptor, error) {
	var desc *sqlbase.TableDescriptor
	if datum == tree.DNull || datum == nil {
		return desc, nil
	}
	descProto := &sqlbase.Descriptor{}
	if col.Name == "descriptor" {
		err := protoutil.Unmarshal([]byte(*datum.(*tree.DBytes)), descProto)
		if err != nil {
			return desc, nil
		}
		switch t := descProto.Union.(type) {
		case *sqlbase.Descriptor_Table:
			desc = descProto.Table(descTs)
			return desc, err
		case *sqlbase.Descriptor_Database, *sqlbase.Descriptor_Schema:
			return desc, nil
		default:
			return desc, errors.Errorf("Descriptor.Union has unexpected type %T", t)
		}
	}
	return desc, nil
}

func parseDescriptorProto(
	ctx context.Context,
	cols []sqlbase.ColumnDescriptor,
	datums sqlbase.EncDatumRow,
	DB *client.DB,
	descTs hlc.Timestamp,
) (sqlbase.DescriptorProto, string, string, error) {
	var desc sqlbase.DescriptorProto
	var databaseName string
	var schemaName string
	if datums == nil {
		return desc, databaseName, schemaName, nil
	}
	descProto := &sqlbase.Descriptor{}
	for i, col := range cols {
		datum := datums[i].Datum
		if col.Name == "descriptor" {
			err := protoutil.Unmarshal([]byte(*datum.(*tree.DBytes)), descProto)
			if err != nil {
				return desc, databaseName, schemaName, err
			}
			switch t := descProto.Union.(type) {
			case *sqlbase.Descriptor_Table:
				desc = descProto.Table(descTs)
				if desc.(*sqlbase.TableDescriptor).ID > keys.PostgresSchemaID {
					schema, err := sqlbase.GetSchemaDescFromID(ctx, DB.NewTxn(ctx, "cdc-ddl-findSchemaName"), desc.(*sqlbase.TableDescriptor).ParentID)
					if err != nil {
						return desc, databaseName, schemaName, err
					}
					schemaName = schema.Name
					database, err := sqlbase.GetDatabaseDescFromID(ctx, DB.NewTxn(ctx, "cdc-ddl-findDatabaseName"), schema.ParentID)
					if err != nil {
						return desc, databaseName, schemaName, err
					}
					databaseName = database.Name
					return desc, databaseName, schemaName, err
				}
			case *sqlbase.Descriptor_Database, *sqlbase.Descriptor_Schema:
				return desc, databaseName, schemaName, nil
			default:
				return desc, databaseName, schemaName, errors.Errorf("Descriptor.Union has unexpected type %T", t)
			}
		}
	}

	return desc, databaseName, schemaName, nil
}

func getPGOidFromColumnType(column sqlbase.ColumnDescriptor) oid.Oid {
	switch column.Type.SemanticType {
	case sqlbase.ColumnType_BOOL:
		return oid.T_bool
	case sqlbase.ColumnType_INT:
		switch column.Type.Width {
		case 16:
			return oid.T_int2
		case 32:
			return oid.T_int4
		case 64, 0:
			return oid.T_int8
		}
	case sqlbase.ColumnType_FLOAT:
		switch column.Type.Width {
		case 32:
			return oid.T_float4
		case 64:
			return oid.T_float8
		}
	case sqlbase.ColumnType_DECIMAL:
		return oid.T_numeric
	case sqlbase.ColumnType_DATE:
		return oid.T_date
	case sqlbase.ColumnType_TIMESTAMP:
		return oid.T_timestamp
	case sqlbase.ColumnType_INTERVAL:
		return oid.T_interval
	case sqlbase.ColumnType_STRING:
		return oid.T_text
	case sqlbase.ColumnType_BYTES:
		return oid.T_bytea
	case sqlbase.ColumnType_TIMESTAMPTZ:
		return oid.T_timestamptz
	case sqlbase.ColumnType_COLLATEDSTRING:
		return oid.T_text
	case sqlbase.ColumnType_NAME:
		return oid.T_name
	case sqlbase.ColumnType_OID:
		return oid.T_oid
	case sqlbase.ColumnType_NULL:
		//TODO: null means which oid
		return 0
	case sqlbase.ColumnType_UUID:
		return oid.T_uuid
	case sqlbase.ColumnType_ARRAY:
		switch *column.Type.ArrayContents {
		case sqlbase.ColumnType_STRING:
			return oid.T__text
		case sqlbase.ColumnType_BIT:
			return oid.T__bit
		case sqlbase.ColumnType_BOOL:
			return oid.T__bool
		case sqlbase.ColumnType_BYTES:
			return oid.T__bytea
		case sqlbase.ColumnType_TIME:
			return oid.T__time
		case sqlbase.ColumnType_JSONB:
			return oid.T__jsonb
		case sqlbase.ColumnType_DECIMAL:
			return oid.T__numeric
		case sqlbase.ColumnType_DATE:
			return oid.T__date
		case sqlbase.ColumnType_TIMESTAMP:
			return oid.T__timestamp
		case sqlbase.ColumnType_TIMESTAMPTZ:
			return oid.T__timestamptz
		case sqlbase.ColumnType_INTERVAL:
			return oid.T__interval
		case sqlbase.ColumnType_OID:
			return oid.T__oid
		case sqlbase.ColumnType_INT2VECTOR:
			return oid.T__int2vector
		case sqlbase.ColumnType_OIDVECTOR:
			return oid.T__oidvector
		case sqlbase.ColumnType_NAME:
			return oid.T__name
		case sqlbase.ColumnType_UUID:
			return oid.T__uuid
		case sqlbase.ColumnType_INT:
			switch column.Type.Width {
			case 16:
				return oid.T__int2
			case 32:
				return oid.T__int4
			case 64, 0:
				return oid.T__int8
			}
		case sqlbase.ColumnType_FLOAT:
			switch column.Type.Width {
			case 32:
				return oid.T__float4
			case 64:
				return oid.T__float8
			}
		}
	case sqlbase.ColumnType_INET:
		return oid.T_inet
	case sqlbase.ColumnType_TIME:
		return oid.T_time
	case sqlbase.ColumnType_JSONB:
		return oid.T_jsonb
	case sqlbase.ColumnType_TUPLE:
		return oid.T_record
	case sqlbase.ColumnType_BIT:
		return oid.T_bit
	case sqlbase.ColumnType_INT2VECTOR:
		return oid.T_int2vector
	case sqlbase.ColumnType_OIDVECTOR:
		return oid.T_oidvector
	}
	return 0
}

func parseDescriptorFromValue(
	output emitEntry, datums sqlbase.EncDatumRow,
) (*sqlbase.Descriptor, error) {
	descProto := &sqlbase.Descriptor{}
	if datums == nil {
		return nil, nil
	}
	for i, col := range output.row.tableDesc.Columns {
		if col.Name == "descriptor" {
			if datum, ok := datums[i].Datum.(*tree.DBytes); ok {
				err := protoutil.Unmarshal([]byte(*datum), descProto)
				if err != nil {
					return descProto, err
				}
			}
		}
	}
	return descProto, nil
}
