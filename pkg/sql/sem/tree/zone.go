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

package tree

// ZoneSpecifier represents a reference to a configurable zone of the keyspace.
type ZoneSpecifier struct {
	// Only one of NamedZone, Database or TableOrIndex may be set.
	NamedZone UnrestrictedName
	Database  Name
	// TODO(radu): TableOrIndex abuses TableIndexName: it allows for the case when
	// an index is not specified, in which case TableOrIndex.Index is empty.
	TableOrIndex TableIndexName

	// Partition is only respected when Table is set.
	Partition Name
}

//ServerTimeZone for db timezone
var ServerTimeZone string

//const (
//	AST = -4
//	ADT = -3
//	BST = 1
//	BDT = 6
//	CDT = -5
//	CST = -6
//	EST = -5
//	EDT = -4
//	GMT = 0
//	HST = -10
//	HDT = -9
//	MST = -7
//	MDT = -6
//	NST = -3.5
//	PST = -8
//	PDT = -7
//	YST = -8
//	YDT = +8
//)

// TargetsTable returns whether the zone specifier targets a table or a subzone
// within a table.
func (node ZoneSpecifier) TargetsTable() bool {
	return node.NamedZone == "" && node.Database == ""
}

// TargetsIndex returns whether the zone specifier targets an index.
func (node ZoneSpecifier) TargetsIndex() bool {
	return node.TargetsTable() && node.TableOrIndex.Index != ""
}

// Format implements the NodeFormatter interface.
func (node *ZoneSpecifier) Format(ctx *FmtCtx) {
	if node.Partition != "" {
		ctx.WriteString("PARTITION ")
		ctx.FormatNode(&node.Partition)
		ctx.WriteString(" OF ")
	}
	if node.NamedZone != "" {
		ctx.WriteString("RANGE ")
		ctx.FormatNode(&node.NamedZone)
	} else if node.Database != "" {
		ctx.WriteString("DATABASE ")
		ctx.FormatNode(&node.Database)
	} else {
		if node.TargetsIndex() {
			ctx.WriteString("INDEX ")
		} else {
			ctx.WriteString("TABLE ")
		}
		ctx.FormatNode(&node.TableOrIndex)
	}
}

func (node *ZoneSpecifier) String() string { return AsString(node) }

// ShowZoneConfig represents a SHOW ZONE CONFIGURATION
// statement.
type ShowZoneConfig struct {
	ZoneSpecifier
}

// Format implements the NodeFormatter interface.
func (n *ShowZoneConfig) Format(ctx *FmtCtx) {
	if n.ZoneSpecifier == (ZoneSpecifier{}) {
		ctx.WriteString("SHOW ZONE CONFIGURATIONS")
	} else {
		ctx.WriteString("SHOW ZONE CONFIGURATION FOR ")
		ctx.FormatNode(&n.ZoneSpecifier)
	}
}

// SetReplica represents a ALTER TABLE REPLICATION
// statement.
type SetReplica struct {
	Table   TableName
	Disable bool
}

// Format implements the NodeFormatter interface.
func (n *SetReplica) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER TABLE ")
	ctx.FormatNode(&n.Table)
	ctx.WriteString(" REPLICATION")
	if n.Disable {
		ctx.WriteString(" DISABLE")
	} else {
		ctx.WriteString(" ENABLE")
	}
}

// SetZoneConfig represents an ALTER DATABASE/TABLE... CONFIGURE ZONE
// statement.
type SetZoneConfig struct {
	ZoneSpecifier
	SetDefault bool
	YAMLConfig Expr
	Options    KVOptions
}

// Format implements the NodeFormatter interface.
func (n *SetZoneConfig) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER ")
	ctx.FormatNode(&n.ZoneSpecifier)
	ctx.WriteString(" CONFIGURE ZONE ")
	if n.SetDefault {
		ctx.WriteString("USING DEFAULT")
	} else if n.YAMLConfig != nil {
		if n.YAMLConfig == DNull {
			ctx.WriteString("DISCARD")
		} else {
			ctx.WriteString("= ")
			ctx.FormatNode(n.YAMLConfig)
		}
	} else {
		ctx.WriteString("USING ")
		kvOptions := n.Options
		comma := ""
		for _, kv := range kvOptions {
			ctx.WriteString(comma)
			comma = ", "
			ctx.FormatNode(&kv.Key)
			if kv.Value != nil {
				ctx.WriteString(` = `)
				ctx.FormatNode(kv.Value)
			} else {
				ctx.WriteString(` = COPY FROM PARENT`)
			}
		}
	}
}

//func Transzone(zone1 string,zone2 string) (float32 ,error){
//	var value1,value2 float32
//	switch  zone1 {
//	case "AST": value1=AST
//	case "ADT": value1=ADT
//	case "BST": value1=BST
//	case "BDT": value1=BDT
//	case "CDT": value1=CDT
//	case "CST": value1=CST
//	case "EST": value1=EST
//	case "EDT": value1=EDT
//	case "GMT": value1=GMT
//	case "HST": value1=HST
//	case "HDT": value1=HDT
//	case "MST": value1=MST
//	case "MDT": value1=MDT
//	case "NST": value1=NST
//	case "PST": value1=PST
//	case "PDT": value1=PDT
//	case "YST": value1=YST
//	case "YDT": value1=YDT
//	default:
//		value1=25
//	}
//	switch  zone2 {
//	case "AST": value2=AST
//	case "ADT": value2=ADT
//	case "BST": value2=BST
//	case "BDT": value2=BDT
//	case "CDT": value2=CDT
//	case "CST": value2=CST
//	case "EST": value2=EST
//	case "EDT": value2=EDT
//	case "GMT": value2=GMT
//	case "HST": value2=HST
//	case "HDT": value2=HDT
//	case "MST": value2=MST
//	case "MDT": value2=MDT
//	case "NST": value2=NST
//	case "PST": value2=PST
//	case "PDT": value2=PDT
//	case "YST": value2=YST
//	case "YDT": value2=YDT
//	default:
//		value2=50
//	}
//	if value2-value1>24 {
//		err := fmt.Errorf("please input right timezone like \"AST\"")
//		return 0,err
//	} else {
//		return value2-value1,nil
//	}
//
//}
