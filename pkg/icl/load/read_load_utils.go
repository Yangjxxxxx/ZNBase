package load

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"strings"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/icl/storageicl"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/security"
	"github.com/znbasedb/znbase/pkg/security/privilege"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sessiondata"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/util/hlc"
)

// ParseDDL 解析出的DDL对应的元数据信息
type ParseDDL struct {
	databases []*sqlbase.DatabaseDescriptor
	schemas   []*sqlbase.SchemaDescriptor
	tables    []*sqlbase.TableDescriptor
}

// ReadParseDDL returns table descriptors for all databases schemas tables  or the
// matching table from SQL statements.
func ReadParseDDL(
	input io.Reader,
	evalCtx *tree.EvalContext,
	settings *cluster.Settings,
	match string,
	parentID sqlbase.ID,
	walltime int64,
	fks fkHandler,
	max int,
	statusServer *serverpb.StatusServer,
	encryption *roachpb.FileEncryptionOptions,
	user string,
) (ParseDDL, error) {
	retDDL := ParseDDL{}
	if encryption != nil {
		bufByte, err := ioutil.ReadAll(input)
		if err != nil {
			return retDDL, err
		}
		byte, err := storageicl.DecryptFile(bufByte, encryption.Key)
		if err != nil {
			return retDDL, err
		}
		input = bytes.NewReader(byte)
	} else {
		var isEncrypted bool
		var err error
		input, isEncrypted, err = storageicl.ReaderAppearsEncrypted(input)
		if err != nil {
			return retDDL, err
		}
		if isEncrypted {
			return retDDL, errors.Errorf("file appears encrypted -- try specifying %q", dumpOptEncPassphrase)
		}
	}
	createDb := make(map[string]*tree.CreateDatabase)
	createSc := make(map[string]*tree.CreateSchema)
	// Modify the CreateTable stmt with the various index additions. We do this
	// instead of creating a full table descriptor first and adding indexes
	// later because MakeSimpleTableDescriptor calls the sql package which calls
	// AllocateIDs which adds the hidden rowid and default primary key. This means
	// we'd have to delete the index and row and modify the column family. This
	// is much easier and probably safer too.
	createTbl := make(map[tree.TableName]*tree.CreateTable)
	createSeq := make(map[tree.TableName]*tree.CreateSequence)
	tableFKs := make(map[tree.TableName][]*tree.ForeignKeyConstraintTableDef)
	ps := newPostgreStream(input, max)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			var retDBs []*sqlbase.DatabaseDescriptor
			var retSCs []*sqlbase.SchemaDescriptor
			{
				for _, db := range createDb {
					newID, err := sql.GenerateUniqueDescID(evalCtx.Ctx(), evalCtx.DB)
					if err != nil {
						return retDDL, err
					}
					dbDesc := makeDatabaseDesc(db, newID, user)
					{
						for _, sc := range createSc {
							if sc.Schema.Catalog() == dbDesc.Name {
								newID, err := sql.GenerateUniqueDescID(evalCtx.Ctx(), evalCtx.DB)
								if err != nil {
									return retDDL, err
								}
								scDesc := makeSchemaDesc(sc, newID, dbDesc.ID, user)
								retSCs = append(retSCs, &scDesc)
								dbDesc.Schemas = append(dbDesc.Schemas, scDesc)
							}
						}
					}
					retDBs = append(retDBs, &dbDesc)
				}
			}
			ret := make([]*sqlbase.TableDescriptor, 0, len(createTbl))
			for name, seq := range createSeq {
				id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
				parentID := getParentID(parentID, retDBs, seq.Name)
				desc, err := sql.MakeSequenceTableDesc(
					name.TableName.String(),
					seq.Options,
					parentID,
					id,
					hlc.Timestamp{WallTime: walltime},
					// todo(xz): owner?
					sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, user),
					settings,
					nil,
				)
				if err != nil {
					return retDDL, err
				}
				fks.resolver[desc.Name] = &desc
				ret = append(ret, desc.TableDesc())
			}
			backrefs := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
			for _, create := range createTbl {
				if create == nil {
					continue
				}
				removeDefaultRegclass(create)
				id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
				parentID := getParentID(parentID, retDBs, create.Table)
				desc, err := MakeSimpleTableDescriptor(evalCtx.Ctx(), settings, create, parentID, id, fks, walltime, statusServer, user)
				if err != nil {
					return retDDL, err
				}
				fks.resolver[desc.Name] = desc
				backrefs[desc.ID] = desc
				ret = append(ret, desc.TableDesc())
			}
			for name, constraints := range tableFKs {
				desc := fks.resolver[name.TableName.String()]
				if desc == nil {
					continue
				}
				for _, constraint := range constraints {
					if err := sql.ResolveFK(evalCtx.Ctx(), nil /* txn */, fks.resolver, desc, constraint, backrefs, sql.NewTable); err != nil {
						return retDDL, err
					}
				}
				if err := fixDescriptorFKState(desc.TableDesc()); err != nil {
					return retDDL, err
				}
			}
			if match != "" && len(ret) != 1 {
				found := make([]string, 0, len(createTbl))
				for name := range createTbl {
					found = append(found, name.TableName.String())
				}
				return retDDL, errors.Errorf("table %q not found in file (found tables: %s)", match, strings.Join(found, ", "))
			}
			//if len(ret) == 0 {
			//	return retDDL, errors.Errorf("no table definition found")
			//}

			{
				retDDL.databases = retDBs
				retDDL.schemas = retSCs
				retDDL.tables = ret
			}

			return retDDL, nil
		}
		if err != nil {
			if pg, ok := pgerror.GetPGCause(err); ok {
				return retDDL, errors.Errorf("%s\n%s", pg.Message, pg.Detail)
			}
			return retDDL, errors.Wrap(err, "postgres parse error")
		}
		switch stmt := stmt.(type) {
		case *tree.CreateDatabase:
			name := stmt.Name.String()
			createDb[name] = stmt
		case *tree.CreateSchema:
			name := stmt.Schema.Schema()
			createSc[name] = stmt
		case *tree.CreateTable:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return retDDL, err
			}
			if match != "" && match != name.TableName.String() {
				createTbl[name] = nil
			} else {
				createTbl[name] = stmt
			}
		case *tree.CreateIndex:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return retDDL, err
			}
			create := createTbl[name]
			if create == nil {
				break
			}
			var idx tree.TableDef = &tree.IndexTableDef{
				Name:        stmt.Name,
				Columns:     stmt.Columns,
				Storing:     stmt.Storing,
				Inverted:    stmt.Inverted,
				Interleave:  stmt.Interleave,
				PartitionBy: stmt.PartitionBy,
			}
			if stmt.Unique {
				idx = &tree.UniqueConstraintTableDef{IndexTableDef: *idx.(*tree.IndexTableDef)}
			}
			create.Defs = append(create.Defs, idx)
		case *tree.AlterTable:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return retDDL, err
			}
			create := createTbl[name]
			if create == nil {
				break
			}
			for _, cmd := range stmt.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAddConstraint:
					switch con := cmd.ConstraintDef.(type) {
					case *tree.ForeignKeyConstraintTableDef:
						if !fks.skip {
							tableFKs[name] = append(tableFKs[name], con)
						}
					default:
						create.Defs = append(create.Defs, cmd.ConstraintDef)
					}
				case *tree.AlterTableSetDefault:
					for i, def := range create.Defs {
						def, ok := def.(*tree.ColumnTableDef)
						if !ok || def.Name != cmd.Column {
							continue
						}
						def.DefaultExpr.Expr = cmd.Default
						create.Defs[i] = def
					}
				case *tree.AlterTableValidateConstraint:
					// ignore
				default:
					return retDDL, errors.Errorf("unsupported statement: %s", stmt)
				}
			}
		case *tree.CreateSequence:
			name, err := getTableName(&stmt.Name)
			if err != nil {
				return retDDL, err
			}
			if match == "" || match == name.TableName.String() {
				createSeq[name] = stmt
			}
		}
	}
}

func readParseDDLFromSchema(
	ctx context.Context,
	p sql.PlanHookState,
	input io.Reader,
	evalCtx *tree.EvalContext,
	settings *cluster.Settings,
	match string,
	parentID sqlbase.ID,
	walltime int64,
	fks fkHandler,
	max int,
	dbName string,
	statusServer *serverpb.StatusServer,
	encryption *roachpb.FileEncryptionOptions,
	user string,
) (ParseDDL, error) {
	retDDL := ParseDDL{}
	if encryption != nil {
		bufByte, err := ioutil.ReadAll(input)
		if err != nil {
			return retDDL, err
		}
		byte, err := storageicl.DecryptFile(bufByte, encryption.Key)
		if err != nil {
			return retDDL, err
		}
		input = bytes.NewReader(byte)
	} else {
		var isEncrypted bool
		var err error
		input, isEncrypted, err = storageicl.ReaderAppearsEncrypted(input)
		if err != nil {
			return retDDL, err
		}
		if isEncrypted {
			return retDDL, errors.Errorf("file appears encrypted -- try specifying %q", dumpOptEncPassphrase)
		}
	}
	createDb := make(map[string]*tree.CreateDatabase)
	createSc := make(map[string]*tree.CreateSchema)
	// Modify the CreateTable stmt with the various index additions. We do this
	// instead of creating a full table descriptor first and adding indexes
	// later because MakeSimpleTableDescriptor calls the sql package which calls
	// AllocateIDs which adds the hidden rowid and default primary key. This means
	// we'd have to delete the index and row and modify the column family. This
	// is much easier and probably safer too.
	createTbl := make(map[tree.TableName]*tree.CreateTable)
	createSeq := make(map[tree.TableName]*tree.CreateSequence)
	tableFKs := make(map[tree.TableName][]*tree.ForeignKeyConstraintTableDef)
	ps := newPostgreStream(input, max)
	for {
		stmt, err := ps.Next()
		if err == io.EOF {
			var retDBs []*sqlbase.DatabaseDescriptor
			var retSCs []*sqlbase.SchemaDescriptor
			{
				dbDesc, err := p.ResolveUncachedDatabaseByName(ctx, dbName, true /*required*/)
				if err != nil {
					return retDDL, err
				}
				{
					for _, sc := range createSc {
						newID, err := sql.GenerateUniqueDescID(evalCtx.Ctx(), evalCtx.DB)
						if err != nil {
							return retDDL, err
						}
						scDesc := makeSchemaDesc(sc, newID, dbDesc.ID, user)
						sc.Schema.SetCatalog(dbDesc.Name)
						retSCs = append(retSCs, &scDesc)
						dbDesc.Schemas = append(dbDesc.Schemas, scDesc)
					}
				}
				retDBs = append(retDBs, dbDesc)
			}
			ret := make([]*sqlbase.TableDescriptor, 0, len(createTbl))
			for name, seq := range createSeq {
				id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
				parentID := getParentID(parentID, retDBs, seq.Name)
				desc, err := sql.MakeSequenceTableDesc(
					name.TableName.String(),
					seq.Options,
					parentID,
					id,
					hlc.Timestamp{WallTime: walltime},
					// todo(xz): owner?
					sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Table, user),
					settings,
					nil,
				)
				if err != nil {
					return retDDL, err
				}
				fks.resolver[desc.Name] = &desc
				ret = append(ret, desc.TableDesc())
			}
			backrefs := make(map[sqlbase.ID]*sqlbase.MutableTableDescriptor)
			for _, create := range createTbl {
				if create == nil {
					continue
				}
				if create.Table.CatalogName == "" {
					create.Table.CatalogName = tree.Name(dbName)
					create.Table.ExplicitCatalog = true
				}
				removeDefaultRegclass(create)
				id := sqlbase.ID(int(defaultCSVTableID) + len(ret))
				parentID := getParentID(parentID, retDBs, create.Table)
				desc, err := MakeSimpleTableDescriptor(evalCtx.Ctx(), settings, create, parentID, id, fks, walltime, statusServer, p.User())
				if err != nil {
					return retDDL, err
				}
				fks.resolver[desc.Name] = desc
				backrefs[desc.ID] = desc
				ret = append(ret, desc.TableDesc())
			}
			for name, constraints := range tableFKs {
				desc := fks.resolver[name.TableName.String()]
				if desc == nil {
					continue
				}
				for _, constraint := range constraints {
					if err := sql.ResolveFK(evalCtx.Ctx(), nil /* txn */, fks.resolver, desc, constraint, backrefs, sql.NewTable); err != nil {
						return retDDL, err
					}
				}
				if err := fixDescriptorFKState(desc.TableDesc()); err != nil {
					return retDDL, err
				}
			}
			if match != "" && len(ret) != 1 {
				found := make([]string, 0, len(createTbl))
				for name := range createTbl {
					found = append(found, name.TableName.String())
				}
				return retDDL, errors.Errorf("table %q not found in file (found tables: %s)", match, strings.Join(found, ", "))
			}
			//if len(ret) == 0 {
			//	return retDDL, errors.Errorf("no table definition found")
			//}

			{
				retDDL.databases = retDBs
				retDDL.schemas = retSCs
				retDDL.tables = ret
			}

			return retDDL, nil
		}
		if err != nil {
			if pg, ok := pgerror.GetPGCause(err); ok {
				return retDDL, errors.Errorf("%s\n%s", pg.Message, pg.Detail)
			}
			return retDDL, errors.Wrap(err, "postgres parse error")
		}
		switch stmt := stmt.(type) {
		case *tree.CreateDatabase:
			name := stmt.Name.String()
			createDb[name] = stmt
		case *tree.CreateSchema:
			name := stmt.Schema.Schema()
			createSc[name] = stmt
		case *tree.CreateTable:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return retDDL, err
			}
			if match != "" && match != name.TableName.String() {
				createTbl[name] = nil
			} else {
				createTbl[name] = stmt
			}
		case *tree.CreateIndex:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return retDDL, err
			}
			create := createTbl[name]
			if create == nil {
				break
			}
			var idx tree.TableDef = &tree.IndexTableDef{
				Name:        stmt.Name,
				Columns:     stmt.Columns,
				Storing:     stmt.Storing,
				Inverted:    stmt.Inverted,
				Interleave:  stmt.Interleave,
				PartitionBy: stmt.PartitionBy,
			}
			if stmt.Unique {
				idx = &tree.UniqueConstraintTableDef{IndexTableDef: *idx.(*tree.IndexTableDef)}
			}
			create.Defs = append(create.Defs, idx)
		case *tree.AlterTable:
			name, err := getTableName(&stmt.Table)
			if err != nil {
				return retDDL, err
			}
			create := createTbl[name]
			if create == nil {
				break
			}
			for _, cmd := range stmt.Cmds {
				switch cmd := cmd.(type) {
				case *tree.AlterTableAddConstraint:
					switch con := cmd.ConstraintDef.(type) {
					case *tree.ForeignKeyConstraintTableDef:
						if !fks.skip {
							tableFKs[name] = append(tableFKs[name], con)
						}
					default:
						create.Defs = append(create.Defs, cmd.ConstraintDef)
					}
				case *tree.AlterTableSetDefault:
					for i, def := range create.Defs {
						def, ok := def.(*tree.ColumnTableDef)
						if !ok || def.Name != cmd.Column {
							continue
						}
						def.DefaultExpr.Expr = cmd.Default
						create.Defs[i] = def
					}
				case *tree.AlterTableValidateConstraint:
					// ignore
				default:
					return retDDL, errors.Errorf("unsupported statement: %s", stmt)
				}
			}
		case *tree.CreateSequence:
			name, err := getTableName(&stmt.Name)
			if err != nil {
				return retDDL, err
			}
			if match == "" || match == name.TableName.String() {
				createSeq[name] = stmt
			}
		}
	}
}

//getParentID 获取schemaID
func getParentID(
	parentID sqlbase.ID, retDBs []*sqlbase.DatabaseDescriptor, name tree.TableName,
) sqlbase.ID {
	schemaID := parentID
	for _, dbDesc := range retDBs {
		if dbDesc.Name == name.Catalog() {
			for _, scDesc := range dbDesc.Schemas {
				if scDesc.Name == name.Schema() {
					schemaID = scDesc.ID
				}
			}
		}
	}
	return schemaID
}

func makeDatabaseDesc(
	p *tree.CreateDatabase, databaseID sqlbase.ID, user string,
) sqlbase.DatabaseDescriptor {
	desc := sqlbase.DatabaseDescriptor{
		Name:       string(p.Name),
		Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Database, user), // todo(xz): owner?
		ID:         databaseID,
	}
	if desc.Name != sqlbase.SystemDB.Name && desc.Name != sessiondata.PgDatabaseName {
		desc.Privileges.Grant(security.RootUser, sqlbase.PublicRole, privilege.List{privilege.USAGE}, false)
	}
	return desc
}

func makeSchemaDesc(
	p *tree.CreateSchema, schemaID sqlbase.ID, databaseID sqlbase.ID, user string,
) sqlbase.SchemaDescriptor {
	return sqlbase.SchemaDescriptor{
		Name:       string(p.Schema.Schema()),
		Privileges: sqlbase.NewDefaultObjectPrivilegeDescriptor(privilege.Schema, user), // todo(xz): user?
		ID:         schemaID,
		ParentID:   databaseID,
	}
}
