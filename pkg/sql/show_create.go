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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// ShowCreate implements the SHOW CREATE statement.
// Privileges: Any privilege on object.
func (p *planner) ShowCreate(ctx context.Context, n *tree.ShowCreate) (planNode, error) {
	// The condition "database_name IS NULL" ensures that virtual tables are included.
	const showCreateQuery = `
     SELECT %[3]s AS table_name,
            create_statement
       FROM %[4]s.zbdb_internal.create_statements
      WHERE (database_name IS NULL OR database_name = %[1]s)
        AND schema_name = %[5]s
        AND descriptor_name = %[2]s
`
	return p.showTableDetails(ctx, "SHOW CREATE", &n.Name, showCreateQuery, p.CheckAnyPrivilege)
}

// ShowCreateView returns a valid SQL representation of the CREATE
// VIEW statement used to create the given view.
func ShowCreateView(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.IsMaterializedView {
		f.WriteString("MATERIALIZED ")
	}
	f.WriteString("VIEW ")
	f.FormatNode(tn)
	f.WriteString(" (")
	for i := range desc.Columns {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&desc.Columns[i].Name)
	}
	f.WriteString(") AS ")
	f.WriteString(desc.ViewQuery)
	return f.CloseAndGetString(), nil
}

func printForeignKeyConstraint(
	ctx context.Context,
	buf *bytes.Buffer,
	dbPrefix string,
	scPrefix string,
	idx *sqlbase.IndexDescriptor,
	lCtx *internalLookupCtx,
) error {
	fk := &idx.ForeignKey
	if !fk.IsSet() {
		return nil
	}
	var refNames []string
	var fkTableName tree.TableName
	if lCtx != nil {
		fkTable, err := lCtx.getTableByID(fk.Table)
		if err != nil {
			return err
		}
		scName := tree.PublicSchema
		var fkDb *DatabaseDescriptor
		if fkSc, err := lCtx.getSchemaByID(fkTable.ParentID); err != nil {
			if fkDb, err = lCtx.getDatabaseByID(fkTable.ParentID); err != nil {
				return err
			}
		} else {
			scName = fkSc.Name
			if fkDb, err = lCtx.getDatabaseByID(fkSc.ParentID); err != nil {
				return err
			}
		}
		fkIdx, err := fkTable.FindIndexByID(fk.Index)
		if err != nil {
			return err
		}
		refNames = fkIdx.ColumnNames
		fkTableName = tree.MakeTableNameWithSchema(tree.Name(fkDb.Name), tree.Name(scName), tree.Name(fkTable.Name))
		if fkDb.Name == dbPrefix {
			fkTableName.ExplicitCatalog = false
			fkTableName.ExplicitSchema = scName != scPrefix
		} else {
			fkTableName.ExplicitCatalog = true
			fkTableName.ExplicitSchema = true
		}

	} else {
		refNames = []string{"???"}
		fkTableName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as ref]", fk.Table)))
		fkTableName.ExplicitCatalog = false
		fkTableName.ExplicitSchema = false
	}
	buf.WriteString("FOREIGN KEY (")
	formatQuoteNames(buf, idx.ColumnNames[0:idx.ForeignKey.SharedPrefixLen]...)
	buf.WriteString(") REFERENCES ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&fkTableName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString(" (")
	formatQuoteNames(buf, refNames...)
	buf.WriteByte(')')
	idx.ColNamesString()
	// We omit MATCH SIMPLE because it is the default.
	if fk.Match != sqlbase.ForeignKeyReference_SIMPLE {
		buf.WriteByte(' ')
		buf.WriteString(fk.Match.String())
	}
	if fk.OnDelete != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != sqlbase.ForeignKeyReference_NO_ACTION {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(fk.OnUpdate.String())
	}
	return nil
}

// ShowCreateSequence returns a valid SQL representation of the
// CREATE SEQUENCE statement used to create the given sequence.
func ShowCreateSequence(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE SEQUENCE ")
	f.FormatNode(tn)
	opts := desc.SequenceOpts
	f.Printf(" MINVALUE %d", opts.MinValue)
	f.Printf(" MAXVALUE %d", opts.MaxValue)
	f.Printf(" INCREMENT %d", opts.Increment)
	f.Printf(" START WITH %d", opts.Start)
	if opts.Cache > 1 {
		f.Printf(" CACHE %d", opts.Cache)
	}
	if opts.Cycle {
		f.Printf(" CYCLE")
	}
	if opts.Virtual {
		f.Printf(" VIRTUAL")
	}
	return f.CloseAndGetString(), nil
}

// ShowCreateTable returns a valid SQL representation of the CREATE
// TABLE statement used to create the given table.
//
// The names of the tables references by foreign keys, and the
// interleaved parent if any, are prefixed by their own database name
// unless it is equal to the given dbPrefix. This allows us to elide
// the prefix when the given table references other tables in the
// current database.
func ShowCreateTable(
	ctx context.Context,
	tn *tree.Name,
	dbPrefix string,
	scPrefix string,
	desc *sqlbase.TableDescriptor,
	lCtx *internalLookupCtx,
	ignoreFKs bool,
	p *planner,
) (string, error) {
	var locationMap *roachpb.LocationMap
	var err error
	partitionMap := map[string]*roachpb.LocationValue{}
	withLocation := false
	if stmt := statementFromCtx(ctx); stmt != nil {
		switch t := stmt.(type) {
		case *tree.ShowCreate:
			if t.WithLocation && t.Name.TableName == *tn {
				withLocation = true
			}
		default:
			// do nothing
		}
	}
	if withLocation {
		locationMap, err = p.GetLocationMap(ctx, desc.ID)
		if err != nil {
			return "", err
		}
		for _, p := range locationMap.PartitionSpace {
			partitionMap[p.Name] = p.Space
		}
	}
	a := &sqlbase.DatumAlloc{}

	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("TABLE ")
	f.FormatNode(tn)
	f.WriteString(" (")
	primaryKeyIsOnVisibleColumn := false
	for i, col := range desc.VisibleColumns() {
		if desc.IsHashPartition {
			if i != 0 && col.Name != "hashnum" {
				f.WriteString(",")
			}
			if col.Name != "hashnum" {
				f.WriteString("\n\t")
				f.WriteString(col.SQLString())
			}
			if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
				// Only set primaryKeyIsOnVisibleColumn to true if the primary key
				// is on a visible column (not rowid).
				primaryKeyIsOnVisibleColumn = true
			}
		} else {
			if i != 0 {
				f.WriteString(",")
			}
			f.WriteString("\n\t")
			f.WriteString(col.SQLString())
			if desc.IsPhysicalTable() && desc.PrimaryIndex.ColumnIDs[0] == col.ID {
				// Only set primaryKeyIsOnVisibleColumn to true if the primary key
				// is on a visible column (not rowid).
				primaryKeyIsOnVisibleColumn = true
			}
		}
	}
	if primaryKeyIsOnVisibleColumn {
		tmp := desc.PrimaryIndex.ColumnNames
		if desc.IsHashPartition {
			if desc.PrimaryIndex.ColumnNames[0] == "hashnum" {
				desc.PrimaryIndex.ColumnNames = desc.PrimaryIndex.ColumnNames[1:]
			}
		}
		f.WriteString(",\n\tCONSTRAINT ")
		formatQuoteNames(&f.Buffer, desc.PrimaryIndex.Name)
		f.WriteString(" ")
		f.WriteString(desc.PrimaryKeyString())
		printIndexLocateIn(&f.Buffer, withLocation, locationMap, &desc.PrimaryIndex)
		desc.PrimaryIndex.ColumnNames = tmp
	}
	allIdx := append(desc.Indexes, desc.PrimaryIndex)
	for i := range allIdx {
		idx := &allIdx[i]
		//if idx.Name == "hashpartitionidx" {
		//	continue
		//}
		if fk := &idx.ForeignKey; fk.IsSet() && !ignoreFKs {
			f.WriteString(",\n\tCONSTRAINT ")
			f.FormatNameP(&fk.Name)
			f.WriteString(" ")
			if err := printForeignKeyConstraint(ctx, &f.Buffer, dbPrefix, scPrefix, idx, lCtx); err != nil {
				return "", err
			}
		}
		if idx.ID != desc.PrimaryIndex.ID || idx.Name == "hashpartitionidx" {
			// Showing the primary index is handled above.
			f.WriteString(",\n\t")
			f.WriteString(idx.SQLString(desc, &sqlbase.AnonymousTable, nil))
			// Showing the INTERLEAVE and PARTITION BY for the primary index are
			// handled last.
			if err := showCreateInterleave(ctx, idx, &f.Buffer, dbPrefix, lCtx); err != nil {
				return "", err
			}
			if err := ShowCreatePartitioning(
				a, desc, idx, &idx.Partitioning, &f.Buffer, 1, 0, partitionMap, withLocation,
			); err != nil {
				return "", err
			}
			printIndexLocateIn(&f.Buffer, withLocation, locationMap, idx)
		}
	}

	for _, fam := range desc.Families {
		activeColumnNames := make([]string, 0, len(fam.ColumnNames))
		for i, colID := range fam.ColumnIDs {
			if _, err := desc.FindActiveColumnByID(colID); err == nil {
				if fam.ColumnNames[i] != "hashnum" {
					activeColumnNames = append(activeColumnNames, fam.ColumnNames[i])
				}
			}
		}
		f.WriteString(",\n\tFAMILY ")
		formatQuoteNames(&f.Buffer, fam.Name)
		f.WriteString(" (")
		formatQuoteNames(&f.Buffer, activeColumnNames...)
		f.WriteString(")")
	}

	for _, e := range desc.AllActiveAndInactiveChecks() {
		f.WriteString(",\n\t")
		if len(e.Name) > 0 {
			f.WriteString("CONSTRAINT ")
			formatQuoteNames(&f.Buffer, e.Name)
			f.WriteString(" ")
		}
		f.WriteString("CHECK (")
		f.WriteString(e.Expr)
		f.WriteString(") ")
		if e.Able {
			f.WriteString("DISABLE")
		} else {
			f.WriteString("ENABLE")
		}
	}

	f.WriteString("\n)")
	err = p.writeInheritInformation(ctx, desc, f)
	if err != nil {
		return "", nil
	}
	if err := showCreateInterleave(ctx, &desc.PrimaryIndex, &f.Buffer, dbPrefix, lCtx); err != nil {
		return "", err
	}
	if err := ShowCreatePartitioning(
		a, desc, &desc.PrimaryIndex, &desc.PrimaryIndex.Partitioning, &f.Buffer, 0, 0, partitionMap, withLocation,
	); err != nil {
		return "", err
	}
	printLocateIn(&f.Buffer, desc.LocateSpaceName)
	if desc.Comments != "" {
		f.WriteString("\n")
		f.WriteString(desc.Comments)
	}
	return f.CloseAndGetString(), nil
}

// formatQuoteNames quotes and adds commas between names.
func formatQuoteNames(buf *bytes.Buffer, names ...string) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	for i := range names {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&names[i])
	}
	buf.WriteString(f.CloseAndGetString())
}

// showCreateInterleave returns an INTERLEAVE IN PARENT clause for the specified
// index, if applicable.
//
// The name of the parent table is prefixed by its database name unless
// it is equal to the given dbPrefix. This allows us to elide the prefix
// when the given index is interleaved in a table of the current database.
func showCreateInterleave(
	ctx context.Context,
	idx *sqlbase.IndexDescriptor,
	buf *bytes.Buffer,
	dbPrefix string,
	lCtx *internalLookupCtx,
) error {
	if len(idx.Interleave.Ancestors) == 0 {
		return nil
	}
	intl := idx.Interleave
	parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID

	var parentName tree.TableName
	if lCtx != nil {
		parentTable, err := lCtx.getTableByID(parentTableID)
		if err != nil {
			return err
		}
		scName := tree.PublicSchema
		var parentDbDesc *DatabaseDescriptor
		if parentScDesc, err := lCtx.getSchemaByID(parentTable.ParentID); err != nil {
			if parentDbDesc, err = lCtx.getDatabaseByID(parentTable.ParentID); err != nil {
				return err
			}
		} else {
			scName = parentScDesc.Name
			if parentDbDesc, err = lCtx.getDatabaseByID(parentScDesc.ParentID); err != nil {
				return err
			}
		}
		parentName = tree.MakeTableNameWithSchema(tree.Name(parentDbDesc.Name), tree.Name(scName), tree.Name(parentTable.Name))
		parentName.ExplicitSchema = parentDbDesc.Name != dbPrefix
	} else {
		parentName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as parent]", parentTableID)))
		parentName.ExplicitCatalog = false
		parentName.ExplicitSchema = false
	}
	var sharedPrefixLen int
	for _, ancestor := range intl.Ancestors {
		sharedPrefixLen += int(ancestor.SharedPrefixLen)
	}
	buf.WriteString(" INTERLEAVE IN PARENT ")
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(&parentName)
	buf.WriteString(fmtCtx.CloseAndGetString())
	buf.WriteString(" (")
	formatQuoteNames(buf, idx.ColumnNames[:sharedPrefixLen]...)
	buf.WriteString(")")
	return nil
}

// ShowCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func ShowCreatePartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
	partitionMap map[string]*roachpb.LocationValue,
	withlocation bool,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	indentStr := strings.Repeat("\t", indent)
	buf.WriteString(` PARTITION BY `)
	if partDesc.IsHashPartition {
		buf.WriteString(`HASH`)
	} else if len(partDesc.List) > 0 {
		buf.WriteString(`LIST`)
	} else if len(partDesc.Range) > 0 {
		buf.WriteString(`RANGE`)
	} else {
		return errors.Errorf(`invalid partition descriptor: %v`, partDesc)
	}
	buf.WriteString(` (`)
	for i := 0; i < int(partDesc.NumColumns); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		if partDesc.IsHashPartition {
			buf.WriteString(tableDesc.HashField)
		} else {
			f := tree.NewFmtCtx(tree.FmtSimple)
			desc := idxDesc.ColumnNames[colOffset+i]
			f.FormatNameP(&desc)
			buf.WriteString(f.CloseAndGetString())
		}
	}
	buf.WriteString(`) (`)
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i := range partDesc.List {
		part := &partDesc.List[i]
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		fmtCtx.FormatNameP(&part.Name)
		_, _ = fmtCtx.Buffer.WriteTo(buf)
		if !tableDesc.IsHashPartition {
			buf.WriteString(` VALUES IN (`)
			for j, values := range part.Values {
				if j != 0 {
					buf.WriteString(`, `)
				}
				tuple, _, err := sqlbase.DecodePartitionTuple(
					a, tableDesc, idxDesc, partDesc, values, fakePrefixDatums)
				if err != nil {
					return err
				}
				buf.WriteString(tuple.String())
			}
			buf.WriteString(`)`)
		}
		if err := ShowCreatePartitioning(
			a, tableDesc, idxDesc, &part.Subpartitioning, buf, indent+1,
			colOffset+int(partDesc.NumColumns), partitionMap, withlocation,
		); err != nil {
			return err
		}
		var spaceName *roachpb.LocationValue
		if withlocation {
			if space, ok := partitionMap[part.Name]; ok {
				spaceName = space
			}
		} else {
			spaceName = part.LocateSpaceName
		}
		printLocateIn(buf, spaceName)
	}
	for i, part := range partDesc.Range {
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		buf.WriteString(part.Name)
		buf.WriteString(" VALUES FROM ")
		fromTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.ToExclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
		var spaceName *roachpb.LocationValue
		if withlocation {
			if space, ok := partitionMap[part.Name]; ok {
				spaceName = space
			}
		} else {
			spaceName = part.LocateSpaceName
		}
		printLocateIn(buf, spaceName)
	}
	buf.WriteString("\n")
	buf.WriteString(indentStr)
	buf.WriteString(")")
	return nil
}

func printLocateIn(buf *bytes.Buffer, space *roachpb.LocationValue) {
	if space == nil {
		return
	}
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	fmtCtx.FormatNode(tree.NewLocation(space))
	_, _ = fmtCtx.Buffer.WriteTo(buf)
}

func printIndexLocateIn(
	buf *bytes.Buffer, withLocation bool, lmap *roachpb.LocationMap, idx *sqlbase.IndexDescriptor,
) {
	var spaceName *roachpb.LocationValue
	if withLocation {
		if space, ok := lmap.IndexSpace[uint32(idx.ID)]; ok {
			spaceName = space
		} else {
			spaceName = lmap.TableSpace
		}
	} else {
		spaceName = idx.LocateSpaceName
	}
	printLocateIn(buf, spaceName)
}

func (p *planner) writeInheritInformation(
	ctx context.Context, desc *sqlbase.TableDescriptor, f *tree.FmtCtx,
) error {
	if len(desc.Inherits) > 0 {
		var str = make([]string, len(desc.Inherits))
		for i, id := range desc.Inherits {
			desc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
			if err != nil || desc == nil {
				return err
			}
			str[i] = desc.Name
		}
		f.WriteString("\nInherits:")
		f.WriteString(strings.Join(str, ", "))
	}

	if len(desc.InheritsBy) > 0 {
		var str = make([]string, len(desc.InheritsBy))
		for i, id := range desc.InheritsBy {
			desc, err := p.Tables().getMutableTableVersionByID(ctx, id, p.txn)
			if err != nil || desc == nil {
				return err
			}
			str[i] = desc.Name
		}
		f.WriteString("\nChild tables:")
		f.WriteString(strings.Join(str, ","))
	}
	return nil
}
