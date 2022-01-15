package tree

import "github.com/znbasedb/znbase/pkg/util/pretty"

// SchemaList is the list of schemaName
type SchemaList []SchemaName

// Format implements the NodeFormatter interface.
func (sl *SchemaList) Format(ctx *FmtCtx) {
	for i := range *sl {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*sl)[i])
	}
}

func (sl *SchemaList) String() string { return AsString(sl) }

func (sl *SchemaList) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*sl))
	for i, n := range *sl {
		d[i] = p.Doc(&n)
	}
	return pretty.Join(",", d...)
}

// SchemaName is the name of database.schema format
type SchemaName struct {
	// schema name
	name Name
	// database name
	catalog         Name
	explicitCatalog bool
}

// Format implements the NodeFormatter interface.
func (sn *SchemaName) Format(ctx *FmtCtx) {
	alwaysFormat := ctx.alwaysFormatTablePrefix()
	if sn.explicitCatalog && alwaysFormat {
		ctx.FormatNode(&sn.catalog)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&sn.name)
}

func (sn *SchemaName) String() string { return AsString(sn) }

// Schema retrieves the unqualified schema name.
func (sn *SchemaName) Schema() string {
	return string(sn.name)
}

// Catalog retrieves the unqualified catalog name.
func (sn *SchemaName) Catalog() string {
	return string(sn.catalog)
}

//SetCatalog Name
func (sn *SchemaName) SetCatalog(dbName string) {
	sn.catalog = Name(dbName)
}

// IsExplicitCatalog return SchemaName`s explicitCatalog.
func (sn *SchemaName) IsExplicitCatalog() bool {
	return sn.explicitCatalog
}

func (sn *SchemaName) doc(p *PrettyCfg) pretty.Doc {
	return p.docAsString(sn)
}

// MakeSchemaName makes the database.schema
func MakeSchemaName(db, schema Name) SchemaName {
	return SchemaName{
		name:            schema,
		catalog:         db,
		explicitCatalog: true,
	}
}

// MakeSchemaFmtName makes the formatted schema name
func MakeSchemaFmtName(dbName string, scName string) string {
	schemaName := MakeSchemaName(Name(dbName), Name(scName))
	fmtCtx := NewFmtCtx(FmtAlwaysQualifyTableNames)
	fmtCtx.FormatNode(&schemaName)
	return fmtCtx.String()
}
