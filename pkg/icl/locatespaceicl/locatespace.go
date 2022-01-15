package locatespaceicl

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/config"
	"github.com/znbasedb/znbase/pkg/icl/utilicl"
	"github.com/znbasedb/znbase/pkg/icl/utilicl/intervalicl"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/uuid"
)

const (
	// listtype represent partition list
	listtype = iota
	// rangetype represent partition range
	rangetype
	exceedColumns = "declared partition columns (%s) exceed the number of columns in index being partitioned (%s)"
	firstColumns  = "declared partition columns (%s) do not match first %d columns in index being partitioned (%s)"
)

var paritionValue = map[string]uint64{
	"minvalue": uint64(sqlbase.PartitionMinVal),
	"maxvalue": uint64(sqlbase.PartitionMaxVal)}

// geo-partition feature add by jiye
type ptColumn partitionColumn

type partitionColumn struct {
	cols           []sqlbase.ColumnDescriptor // save columndescriptor
	listSubPtCols  []ptColumn                 // list-sub-partition column descriptor
	rangeSubPtCols []ptColumn                 // range-sub-partition column descriptor
}

// checkLocateSpaceNameExist will check this Locate space if exists
func checkLocateSpaceNameExist(
	ctx context.Context, locateSpaceName *roachpb.LocationValue, server serverpb.StatusServer,
) error {
	if locateSpaceName == nil {
		return nil
	}
	removeDuplicateValues(locateSpaceName)
	if server == nil {
		return errors.New("Can not get status of this cluster")
	}
	nodes, err := server.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}
	for _, space := range locateSpaceName.Spaces {
		find := false
		if space == "" {
			find = true
			break
		}
		for _, node := range nodes.Nodes {
			nameList := node.Desc.LocationName.Names
			for _, name := range nameList {
				if ok := strings.Compare(name, space); ok == 0 {
					find = true
					break
				}
			}
			if find {
				break
			}
		}
		if !find {
			return fmt.Errorf("locate space %s not exist", space)
		}
	}
	return checkLeaseSpaceNameExist(locateSpaceName, nodes)
}

// checkLeaseSpaceNameExist will check this Lease spaces if exist in Locate spaces
func checkLeaseSpaceNameExist(
	locateSpaceName *roachpb.LocationValue, nodes *serverpb.NodesResponse,
) error {
	if locateSpaceName == nil || len(locateSpaceName.Leases) == 0 {
		return nil
	}
	if len(locateSpaceName.Spaces) == 0 {
		return fmt.Errorf("locate space not defined")
	}
	for _, lease := range locateSpaceName.Leases {
		find := false
		for _, space := range locateSpaceName.Spaces {
			// 可以直接在LOCATE IN 中找到LEASE IN的space
			if lease == space {
				find = true
				break
			}
			// 需要通过space找到对应node的所有space来判断LEASE IN是否存在
			nodeSpaces := GetLocationNodespaces(space, nodes)
			for _, nodeSpace := range nodeSpaces {
				if lease == nodeSpace {
					find = true
					break
				}
			}
			if find {
				break
			}
		}
		if !find {
			return fmt.Errorf("lease space %s not exist in locate space %s", lease, locateSpaceName.Spaces)
		}
	}
	return nil
}

// removeDuplicateValues is to remove duplicate elements in spaces and leases
func removeDuplicateValues(locateSpaceName *roachpb.LocationValue) {
	if locateSpaceName == nil {
		return
	}
	locateSpaceName.Spaces = removeLoop(locateSpaceName.Spaces)
	locateSpaceName.Leases = removeLoop(locateSpaceName.Leases)
}

// removeLoop is to duplicate elements
func removeLoop(slc []string) []string {
	var result []string
	for i := range slc {
		flag := true
		for j := range result {
			if slc[i] == result[j] {
				flag = false
				break
			}
		}
		if flag {
			result = append(result, slc[i])
		}
	}
	if len(result) > 1 {
		for i, v := range result {
			if v == "" {
				result = append(result[:i], result[i+1:]...)
				return result
			}
		}
	}
	return result
}

// GetLocationNodespaces returns all spaces in the nodes
func GetLocationNodespaces(space string, nodes *serverpb.NodesResponse) (spaces []string) {
	for _, node := range nodes.Nodes {
		for _, name := range node.Desc.LocationName.Names {
			if space == name {
				spaces = append(spaces, node.Desc.LocationName.Names...)
			}
		}
	}
	return spaces
}

// exprTransferToByte will check input expr and turn it into an byte array
func exprTransferToByte(
	_ context.Context,
	evalCtx *tree.EvalContext,
	prtType int,
	expr tree.Expr,
	cols []sqlbase.ColumnDescriptor,
) ([]byte, error) {
	var value []byte
	var err error
	var scratch []byte
	// 1. for echo expr first convert it to an type expr
	// test expr if a tuple
	exprTuple, ok := expr.(*tree.Tuple)
	if !ok { // not a tuple
		exprTuple = &tree.Tuple{Exprs: []tree.Expr{expr}} // if not a tuple ,will construct an one unit tuple
	}
	// 2. if tuple number not match pby column number will return error
	if len(exprTuple.Exprs) != len(cols) {
		return value, errors.Errorf("value does not match with partition by columns")
	}

	var semaContext = tree.SemaContext{}
	for i, e := range exprTuple.Exprs {
		eExpr := tree.StripParens(e)
		switch t := eExpr.(type) {
		case tree.DefaultVal:
			if prtType == rangetype {
				return value, errors.Errorf("DEFAULT cannot be used with PARTITION BY RANGE")
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode low
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(sqlbase.PartitionDefaultVal))
			continue
		case *tree.UnresolvedName:
			var val uint64
			if t.NumParts == 1 {
				val, ok = paritionValue[t.Parts[0]]
				if !ok {
					return value, errors.Errorf("%s cannot be used with PRTITION BY LIST", expr)
				}
			}
			if prtType == listtype {
				return value, errors.Errorf("%s cannot be used with PARTITION BY LIST",
					strings.ToUpper(t.Parts[0]))
			}

			// NOT NULL is used to signal that a PartitionSpecialValCode low
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, val)
			continue
		default:
			// default value will do as follow
		}
		typeExpr, err := sqlbase.SanitizeVarFreeExpr(e, cols[i].Type.ToDatumType(),
			"partition", &semaContext, false, false)
		if err != nil {
			return value, err
		}

		datum, err := typeExpr.Eval(evalCtx)
		if err != nil {
			return value, err
		}
		// check datum if fit to related column
		if err = sqlbase.CheckDatumTypeFitsColumnType(cols[i], datum.ResolvedType(), nil); err != nil {
			return value, err
		}
		// encoding this datum to an encoding bytes
		value, err = sqlbase.EncodeTableValue(value, cols[i].ID, datum, scratch)
		if err != nil {
			return value, err
		}
	}
	return value, err
}

// generatePartitionColumns will generate the Partition column used for check and construct about descriptor of partition
func generatePartitionColumns(
	partBy *tree.PartitionBy,
	indexDesc *sqlbase.IndexDescriptor,
	tableDesc *sqlbase.MutableTableDescriptor,
) (ptColumn, error) {
	// Get partition by column list from partBy
	var err error
	ptColums := ptColumn{}
	ptColums.cols = make([]sqlbase.ColumnDescriptor, 0)
	if tableDesc.IsHashPartition && len(tableDesc.Mutations) == 1 && tableDesc.Mutations[0].GetColumn() != nil {
		if tableDesc.Mutations[0].GetColumn().Name == "hashnum" {
			col := tableDesc.Mutations[0].GetColumn()
			ptColums.cols = append(ptColums.cols, *col)
		}
	} else {
		for _, fieldName := range partBy.Fields {
			col, err := tableDesc.FindActiveColumnByName(string(fieldName))
			if err != nil {
				return ptColums, err
			}
			ptColums.cols = append(ptColums.cols, col)
		}
	}
	if len(partBy.List) != 0 {
		ptColums.listSubPtCols = make([]ptColumn, len(partBy.List))
		// do with list
		for m, list := range partBy.List {
			if list.Subpartition != nil {
				ptColums.listSubPtCols[m], err = generatePartitionColumns(list.Subpartition, indexDesc, tableDesc)
				if err != nil {
					return ptColums, err
				}
			}
		}
	}
	if len(partBy.Range) != 0 {
		ptColums.rangeSubPtCols = make([]ptColumn, len(partBy.Range))
		// do with range
		for m, rangeList := range partBy.Range {
			if rangeList.Subpartition != nil {
				ptColums.rangeSubPtCols[m], err = generatePartitionColumns(rangeList.Subpartition, indexDesc, tableDesc)
				if err != nil {
					return ptColums, err
				}
			}
		}
	}

	return ptColums, err
}

// checkPartitionOrderWithIndex check partition by field and index column
func checkPartitionOrderWithIndex(
	ptColumns ptColumn, indexDesc *sqlbase.IndexDescriptor, colOffset int,
) (bool, error) {
	valid := true
	var err error
	cols := ptColumns.cols

	for offset, col := range cols {
		i := offset + colOffset + 1
		if i > len(indexDesc.ColumnNames) {
			return false, errors.Errorf(exceedColumns,
				func() string {
					pCols := append([]string(nil), indexDesc.ColumnNames[:colOffset]...)
					for _, p := range cols {
						pCols = append(pCols, p.Name)
					}
					return strings.Join(pCols, ", ")
				}(), strings.Join(indexDesc.ColumnNames, ", "))
		}
		if col.Name != indexDesc.ColumnNames[i-1] {
			return false, errors.Errorf(firstColumns,
				func() string {
					pCols := append([]string(nil), indexDesc.ColumnNames[:colOffset]...)
					for _, p := range cols {
						pCols = append(pCols, p.Name)
					}
					return strings.Join(pCols, ", ")
				}(), i, strings.Join(indexDesc.ColumnNames[:i], ", "))
		}
	}

	for _, list := range ptColumns.listSubPtCols {
		valid, err = checkPartitionOrderWithIndex(list, indexDesc, colOffset+len(cols))
		if !valid {
			return false, err
		}
	}

	for _, list := range ptColumns.rangeSubPtCols {
		//if len(cols) > 1 {
		//	return false, errors.New("Range cols cannot exceed 1.")
		//}
		valid, err = checkPartitionOrderWithIndex(list, indexDesc, colOffset+len(cols))
		if !valid {
			return false, err
		}
	}
	return valid, nil
}

// createPartitionning is the first interface of inspur licenese abourt geo-partitioning
func createPartitionning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
	statusServer *serverpb.StatusServer,
) (sqlbase.PartitioningDescriptor, error) {
	var err error
	// to do : inspur license check need to add
	ptColumns, err := generatePartitionColumns(partBy, indexDesc, tableDesc)
	if err != nil {
		return sqlbase.PartitioningDescriptor{}, err
	}
	// check valid ptColumns with index order
	if ok, err := checkPartitionOrderWithIndex(ptColumns, indexDesc, 0); !ok {
		return sqlbase.PartitioningDescriptor{}, err
	}
	return createPartitionningLater(ctx, st, evalCtx, tableDesc, indexDesc, ptColumns, partBy, statusServer)
}

// createPartitionning transfer AST(PartitionBy) to PartitionDescriptor in icl license hook function
func createPartitionningLater(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	ptColumns ptColumn,
	partBy *tree.PartitionBy,
	statusServer *serverpb.StatusServer,
) (sqlbase.PartitioningDescriptor, error) {

	var err error
	partitionDesc := sqlbase.PartitioningDescriptor{}
	partitionDesc.NumColumns = uint32(len(partBy.Fields))
	cols := ptColumns.cols
	// first traverse the list of this partition by
	for m, list := range partBy.List {

		partitionName := tree.Name(list.Name)
		locateName := list.LocateSpaceName.ToValue()

		// to do check the locatespace exist
		if err := checkLocateSpaceNameExist(ctx, locateName, *statusServer); err != nil {
			return partitionDesc, err
		}
		if locateName != nil {
			partitionDesc.LocationNums++
		}
		// construct partitiondescriptor_list
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		partitionName.Format(fmtCtx)
		ptName := fmtCtx.String()
		ptbList := sqlbase.PartitioningDescriptor_List{
			Name:            ptName,
			LocateSpaceName: locateName,
		}
		//construct value by encoding the of exprs from partitionby list
		if tableDesc.IsHashPartition && partBy.IsHash {
			list.Exprs = tree.Exprs{tree.NewDInt(tree.DInt(m))}
		}
		valueExpr := list.Exprs
		value := make([][]byte, len(valueExpr))
		var err error
		for m, expr := range valueExpr {
			value[m], err = exprTransferToByte(ctx, evalCtx, listtype, expr, cols)
			if err != nil {
				return partitionDesc, errors.Errorf("PARTITION %s's %s", ptbList.Name, err.Error())
			}
		}
		ptbList.Values = value
		if list.Subpartition != nil {
			// todo: partition's subpartition
			ptbList.Subpartitioning, err = createPartitionningLater(ctx, st, evalCtx, tableDesc,
				indexDesc, ptColumns.listSubPtCols[m], list.Subpartition, statusServer)
			if err != nil {
				return partitionDesc, err
			}
			partitionDesc.LocationNums += ptbList.Subpartitioning.LocationNums
		}
		partitionDesc.List = append(partitionDesc.List, ptbList)
	}

	// second do range list construct
	for _, rangeList := range partBy.Range {
		partitionName := tree.Name(rangeList.Name)
		locateName := rangeList.LocateSpaceName.ToValue()

		// to do check the locatespace exist
		if err := checkLocateSpaceNameExist(ctx, locateName, *statusServer); err != nil {
			return partitionDesc, err
		}

		if locateName != nil {
			partitionDesc.LocationNums++
		}
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		partitionName.Format(fmtCtx)
		ptName := fmtCtx.String()
		rangeListDesc := sqlbase.PartitioningDescriptor_Range{
			Name:            ptName,
			LocateSpaceName: locateName,
		}

		fromExpr := rangeList.From
		toExpr := rangeList.To
		rangeListDesc.FromInclusive, err = exprTransferToByte(ctx, evalCtx, rangetype, &tree.Tuple{Exprs: fromExpr}, cols)
		if err != nil {
			return partitionDesc, errors.Errorf("PARTITION %s's %s", rangeListDesc.Name, err.Error())
		}
		rangeListDesc.ToExclusive, err = exprTransferToByte(ctx, evalCtx, rangetype, &tree.Tuple{Exprs: toExpr}, cols)
		if err != nil {
			return partitionDesc, errors.Errorf("PARTITION %s's %s", rangeListDesc.Name, err.Error())
		}
		if rangeList.Subpartition != nil {
			return partitionDesc, errors.Errorf("PARTITION %s: cannot subpartition a range partition ", rangeList.Name)
		}
		partitionDesc.Range = append(partitionDesc.Range, rangeListDesc)
	}
	if partBy.IsHash {
		partitionDesc.IsHashPartition = true
	}
	return partitionDesc, nil
}

// getPartitionSpaceICL is encode PartitioningDescriptor to *LocationMap_PartitionSpace
func getPartitionSpaceICL(
	a *sqlbase.DatumAlloc,
	td *sqlbase.TableDescriptor,
	id *sqlbase.IndexDescriptor,
	pd *sqlbase.PartitioningDescriptor,
	spaceName *roachpb.LocationValue,
	prefix []tree.Datum,
) ([]*roachpb.LocationMap_PartitionSpace, error) {
	if pd.NumColumns == 0 {
		return nil, nil
	}
	partitionSpaces := make([]*roachpb.LocationMap_PartitionSpace, 0)
	if len(pd.List) > 0 {
		for _, p := range pd.List {
			for _, buf := range p.Values {
				t, key, err := sqlbase.DecodePartitionTuple(a, td, id, pd, buf, prefix)
				if err != nil {
					return nil, err
				}
				tmpName := spaceName
				if p.LocateSpaceName != nil {
					for i, s := range p.LocateSpaceName.Spaces {
						if s == "" && len(p.LocateSpaceName.Spaces) == 1 {
							p.LocateSpaceName.Spaces = nil
						} else if s == "" {
							p.LocateSpaceName.Spaces = append(p.LocateSpaceName.Spaces[:i], p.LocateSpaceName.Spaces[i+1:]...)
						}
					}
				}
				if p.LocateSpaceName != nil && len(p.LocateSpaceName.Spaces) != 0 {
					tmpName = p.LocateSpaceName
				}
				partitionSpaces = append(partitionSpaces, &roachpb.LocationMap_PartitionSpace{
					Name:     p.Name,
					StartKey: key,
					EndKey:   roachpb.RKey(key).PrefixEnd(),
					Space:    tmpName,
					IndexID:  uint32(id.ID),
				})
				// 当partitionby的values为default时, t.datums会解析为nil,导致子分区的value在decode时会出现问题.因此在这里补一个t.datums的长度,但补充dnull不知是否妥当.
				if len(t.Datums) == 0 {
					t.Datums = append(t.Datums, tree.DNull)
				}
				newPrefix := append(prefix, t.Datums...)
				subPartition, err := getPartitionSpaceICL(a, td, id, &p.Subpartitioning, tmpName, newPrefix)
				if err != nil {
					return nil, err
				}
				partitionSpaces = append(partitionSpaces, subPartition...)
			}
		}
	}
	if len(pd.Range) > 0 {
		for _, p := range pd.Range {
			_, fromKey, err := sqlbase.DecodePartitionTuple(a, td, id, pd, p.FromInclusive, prefix)
			if err != nil {
				return nil, err
			}
			_, toKey, err := sqlbase.DecodePartitionTuple(a, td, id, pd, p.ToExclusive, prefix)
			if err != nil {
				return nil, err
			}
			//by lz
			//fix bug31, like row384
			tmpName := spaceName
			if p.LocateSpaceName != nil {
				for i, s := range p.LocateSpaceName.Spaces {
					if s == "" && len(p.LocateSpaceName.Spaces) == 1 {
						p.LocateSpaceName.Spaces = nil
					} else if s == "" {
						p.LocateSpaceName.Spaces = append(p.LocateSpaceName.Spaces[:i], p.LocateSpaceName.Spaces[i+1:]...)
					}
				}
			}
			if p.LocateSpaceName != nil && len(p.LocateSpaceName.Spaces) != 0 {
				tmpName = p.LocateSpaceName
			}
			partitionSpaces = append(partitionSpaces, &roachpb.LocationMap_PartitionSpace{
				Name:     p.Name,
				StartKey: fromKey,
				EndKey:   toKey,
				Space:    tmpName,
				IndexID:  uint32(id.ID),
			})
		}
	}
	sort.Slice(partitionSpaces, func(i, j int) bool {
		return bytes.Compare(partitionSpaces[i].StartKey,
			partitionSpaces[j].StartKey) == -1
	})

	return partitionSpaces, nil
}

// GetLocationMapICL is encode TableDescriptor to *LocationMap
func GetLocationMapICL(td *sqlbase.TableDescriptor) (*roachpb.LocationMap, error) {
	var spaceName *roachpb.LocationValue
	if td == nil {
		return nil, errors.Errorf("TableDescriptor should not be nil")
	}
	locationMap := roachpb.NewLocationMap()
	locationMap.TableSpace = td.LocateSpaceName
	indexes := []sqlbase.IndexDescriptor{td.PrimaryIndex}
	indexes = append(indexes, td.Indexes...)
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].ID < indexes[j].ID
	})

	for _, index := range indexes {
		if index.LocateSpaceName == nil || len(index.LocateSpaceName.Spaces) == 0 {
			spaceName = td.LocateSpaceName
		} else {
			spaceName = index.LocateSpaceName
		}
		locationMap.IndexSpace[uint32(index.ID)] = spaceName
		var emptyPrefix []tree.Datum
		partitionSpace, err := getPartitionSpaceICL(&sqlbase.DatumAlloc{}, td, &index, &index.Partitioning, spaceName, emptyPrefix)
		if err != nil {
			return nil, err
		}
		locationMap.PartitionSpace = append(locationMap.PartitionSpace, partitionSpace...)
	}
	return locationMap, nil
}

// GenerateSubzoneSpans generates subzone spans for subzones.
func GenerateSubzoneSpans(
	st *cluster.Settings,
	clusterID uuid.UUID,
	tableDesc *sqlbase.TableDescriptor,
	subzones []config.Subzone,
	hasNewSubzones bool,
) ([]config.SubzoneSpan, error) {
	// Removing zone configs does not require a valid license.
	if hasNewSubzones {
		org := sql.ClusterOrganization.Get(&st.SV)
		if err := utilicl.CheckCommercialFeatureEnabled(st, clusterID, org, "replication zones on indexes or partitions"); err != nil {
			return nil, err
		}
	}

	a := &sqlbase.DatumAlloc{}

	subzoneIndexByIndexID := make(map[sqlbase.IndexID]int32)
	subzoneIndexByPartition := make(map[string]int32)
	for i, subzone := range subzones {
		if len(subzone.PartitionName) > 0 {
			subzoneIndexByPartition[subzone.PartitionName] = int32(i)
		} else {
			subzoneIndexByIndexID[sqlbase.IndexID(subzone.IndexID)] = int32(i)
		}
	}

	var indexCovering intervalicl.Ranges
	var partitionCoverings []intervalicl.Ranges
	if err := tableDesc.ForeachNonDropIndex(func(idxDesc *sqlbase.IndexDescriptor) error {
		_, indexSubzoneExists := subzoneIndexByIndexID[idxDesc.ID]
		if indexSubzoneExists {
			idxSpan := tableDesc.IndexSpan(idxDesc.ID)
			// Each index starts with a unique prefix, so (from a precedence
			// perspective) it's safe to append them all together.
			indexCovering = append(indexCovering, intervalicl.Range{
				Low: idxSpan.Key, High: idxSpan.EndKey,
				Payload: config.Subzone{IndexID: uint32(idxDesc.ID)},
			})
		}

		var emptyPrefix []tree.Datum
		indexPartitionCoverings, err := indexCoveringsForPartitioning(
			a, tableDesc, idxDesc, &idxDesc.Partitioning, subzoneIndexByPartition, emptyPrefix)
		if err != nil {
			return err
		}
		// The returned indexPartitionCoverings are sorted with highest
		// precedence first. They all start with the index prefix, so cannot
		// overlap with the partition coverings for any other index, so (from a
		// precedence perspective) it's safe to append them all together.
		partitionCoverings = append(partitionCoverings, indexPartitionCoverings...)

		return nil
	}); err != nil {
		return nil, err
	}

	// OverlapCoveringMerge returns the payloads for any coverings that overlap
	// in the same order they were input. So, we require that they be ordered
	// with highest precedence first, so the first payload of each range is the
	// one we need.
	ranges := intervalicl.OverlapCoveringMerge(append(partitionCoverings, indexCovering))

	// NB: This assumes that none of the indexes are interleaved, which is
	// checked in PartitionDescriptor validation.
	sharedPrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))

	var subzoneSpans []config.SubzoneSpan
	for _, r := range ranges {
		payloads := r.Payload.([]interface{})
		if len(payloads) == 0 {
			continue
		}
		subzoneSpan := config.SubzoneSpan{
			Key:    bytes.TrimPrefix(r.Low, sharedPrefix),
			EndKey: bytes.TrimPrefix(r.High, sharedPrefix),
		}
		var ok bool
		if subzone := payloads[0].(config.Subzone); len(subzone.PartitionName) > 0 {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByPartition[subzone.PartitionName]
		} else {
			subzoneSpan.SubzoneIndex, ok = subzoneIndexByIndexID[sqlbase.IndexID(subzone.IndexID)]
		}
		if !ok {
			continue
		}
		if bytes.Equal(subzoneSpan.Key.PrefixEnd(), subzoneSpan.EndKey) {
			subzoneSpan.EndKey = nil
		}
		subzoneSpans = append(subzoneSpans, subzoneSpan)
	}
	return subzoneSpans, nil
}

func indexCoveringsForPartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	relevantPartitions map[string]int32,
	prefixDatums []tree.Datum,
) ([]intervalicl.Ranges, error) {
	if partDesc.NumColumns == 0 {
		return nil, nil
	}

	var coverings []intervalicl.Ranges
	var descendentCoverings []intervalicl.Ranges

	if len(partDesc.List) > 0 {
		// The returned spans are required to be ordered with highest precedence
		// first. The span for (1, DEFAULT) overlaps with (1, 2) and needs to be
		// returned at a lower precedence. Luckily, because of the partitioning
		// validation, we're guaranteed that all entries in a list partitioning
		// with the same number of DEFAULTs are non-overlapping. So, bucket the
		// `intervalccl.Range`s by the number of non-DEFAULT columns and return
		// them ordered from least # of DEFAULTs to most.
		listCoverings := make([]intervalicl.Ranges, int(partDesc.NumColumns)+1)
		for _, p := range partDesc.List {
			for _, valueEncBuf := range p.Values {
				t, keyPrefix, err := sqlbase.DecodePartitionTuple(
					a, tableDesc, idxDesc, partDesc, valueEncBuf, prefixDatums)
				if err != nil {
					return nil, err
				}
				if _, ok := relevantPartitions[p.Name]; ok {
					listCoverings[len(t.Datums)] = append(listCoverings[len(t.Datums)], intervalicl.Range{
						Low: keyPrefix, High: roachpb.Key(keyPrefix).PrefixEnd(),
						Payload: config.Subzone{PartitionName: p.Name},
					})
				}
				newPrefixDatums := append(prefixDatums, t.Datums...)
				subpartitionCoverings, err := indexCoveringsForPartitioning(
					a, tableDesc, idxDesc, &p.Subpartitioning, relevantPartitions, newPrefixDatums)
				if err != nil {
					return nil, err
				}
				descendentCoverings = append(descendentCoverings, subpartitionCoverings...)
			}
		}
		for i := range listCoverings {
			if covering := listCoverings[len(listCoverings)-i-1]; len(covering) > 0 {
				coverings = append(coverings, covering)
			}
		}
	}

	if len(partDesc.Range) > 0 {
		for _, p := range partDesc.Range {
			if _, ok := relevantPartitions[p.Name]; !ok {
				continue
			}
			_, fromKey, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, p.FromInclusive, prefixDatums)
			if err != nil {
				return nil, err
			}
			_, toKey, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, p.ToExclusive, prefixDatums)
			if err != nil {
				return nil, err
			}
			if _, ok := relevantPartitions[p.Name]; ok {
				coverings = append(coverings, intervalicl.Ranges{{
					Low: fromKey, High: toKey,
					Payload: config.Subzone{PartitionName: p.Name},
				}})
			}
		}
	}

	// descendentCoverings are from subpartitions and so get precedence; append
	// them to the front.
	return append(descendentCoverings, coverings...), nil
}

func init() {
	sql.CreatePartitionningICL = createPartitionning
	sql.CheckLocateSpaceNameExistICL = checkLocateSpaceNameExist
	sql.GetLocationMapICL = GetLocationMapICL
	sql.GenerateSubzoneSpans = GenerateSubzoneSpans
}
