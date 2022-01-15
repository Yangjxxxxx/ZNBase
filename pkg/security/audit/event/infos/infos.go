package infos

import (
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/util/uuid"
)

//NodeInfo info struct
type NodeInfo struct {
	NodeID    int32 `protobuf:"varint,1,opt,name=node_id,json=nodeId,casttype=NodeID" json:"node_id"`
	LastUp    int64
	ClusterID uuid.UUID
}

//String print info in mail
func (info *NodeInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "nodeID:%d, lastUp:%d, ClusterID:%v", info.NodeID, info.LastUp, info.ClusterID)
	return sb.String()
}

//CreateTableInfo info struct
type CreateTableInfo struct {
	TableName string
	Statement string
	User      string
}

//String print info in mail
func (info *CreateTableInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "TableName:%s,Statement:%s,User:%s", info.TableName, info.Statement, info.User)
	return sb.String()
}

//AlterTableInfo info struct
type AlterTableInfo struct {
	TableName  string
	Statement  string
	User       string
	MutationID uint32
}

//String print info in mail
func (info *AlterTableInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "TableName:%s,Statement:%s,User:%s,MutationID:%d", info.TableName, info.Statement, info.User, info.MutationID)
	return sb.String()
}

//DropTableInfo info struct
type DropTableInfo struct {
	TableName           string
	Statement           string
	User                string
	CascadeDroppedViews []string
}

//String print info in mail
func (info *DropTableInfo) String() string {
	var sb strings.Builder
	var temp string
	var value string
	for _, value = range info.CascadeDroppedViews {
		temp = temp + " " + value
	}
	if temp != "" {
		_, _ = fmt.Fprintf(&sb, "TableName:%s,Statement:%s,User:%s,CascadeDroppedViews:%v", info.TableName, info.Statement, info.User, temp)
	} else {
		_, _ = fmt.Fprintf(&sb, "TableName:%s,Statement:%s,User:%s", info.TableName, info.Statement, info.User)
	}
	return sb.String()
}

//CreateIndexInfo info struct
type CreateIndexInfo struct {
	TableName  string
	IndexName  string
	Statement  string
	User       string
	MutationID uint32
}

//String print info in mail
func (info *CreateIndexInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "TableName:%s,IndexName:%s,Statement:%s,User:%s,MutationID:%d", info.TableName, info.IndexName, info.Statement, info.User, info.MutationID)
	return sb.String()
}

//DropIndexInfo info struct
type DropIndexInfo struct {
	TableName           string
	IndexName           string
	Statement           string
	User                string
	MutationID          uint32
	CascadeDroppedViews []string
}

//String print info in mail
func (info *DropIndexInfo) String() string {
	var sb strings.Builder
	var temp string
	var value string
	for _, value = range info.CascadeDroppedViews {
		temp = temp + " " + value
	}
	if temp != "" {
		_, _ = fmt.Fprintf(&sb, "TableName:%s,IndexName:%s,Statement:%s,User:%s,MutationID:%d,CascadeDroppedViews:%v", info.TableName, info.IndexName, info.Statement,
			info.User, info.MutationID, temp)
	} else {
		_, _ = fmt.Fprintf(&sb, "TableName:%s,IndexName:%s,Statement:%s,User:%s,MutationID:%d", info.TableName, info.IndexName, info.Statement, info.User, info.MutationID)
	}
	return sb.String()
}

//TruncateInfo info struct
type TruncateInfo struct {
	TableName string
	Statement string
	User      string
}

//String print info in mail
func (info *TruncateInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "TableName:%s,Statement:%s,User:%s", info.TableName, info.Statement, info.User)
	return sb.String()
}

//CreateDatabaseInfo info struct
type CreateDatabaseInfo struct {
	DatabaseName string
	Statement    string
	User         string
}

//String print info in mail
func (info *CreateDatabaseInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "DatabaseName:%s,Statement:%s,User:%s", info.DatabaseName, info.Statement, info.User)
	return sb.String()
}

//DropDatabaseInfo info struct
type DropDatabaseInfo struct {
	DatabaseName         string
	Statement            string
	User                 string
	DroppedSchemaObjects []string
}

//String print info in mail
func (info *DropDatabaseInfo) String() string {
	var sb strings.Builder
	var temp string
	var value string
	for _, value = range info.DroppedSchemaObjects {
		temp = temp + " " + value
	}
	if temp != "" {
		_, _ = fmt.Fprintf(&sb, "DatabaseName:%s,Statement:%s,User:%s,DroppedSchemaObjects:%v", info.DatabaseName, info.Statement, info.User, temp)
	} else {
		_, _ = fmt.Fprintf(&sb, "DatabaseName:%s,Statement:%s,User:%s", info.DatabaseName, info.Statement, info.User)
	}
	return sb.String()
}

//SetClusterSettingInfo info struct
type SetClusterSettingInfo struct {
	SettingName string
	Value       string
	User        string
}

//String print info in mail
func (info *SetClusterSettingInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "SettingName:%s,Value:%s,User:%s", info.SettingName, info.Value, info.User)
	return sb.String()
}

//SetZoneConfigInfo info struct
type SetZoneConfigInfo struct {
	Target  string
	Config  string `json:",omitempty"`
	Options string `json:",omitempty"`
	User    string
}

//String print info in mail
func (info *SetZoneConfigInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "Target:%s,Config:%s,Options:%s,User:%s", info.Target, info.Config, info.Options, info.User)
	return sb.String()
}

//CreateSequenceInfo info struct
type CreateSequenceInfo struct {
	SequenceName string
	Statement    string
	User         string
}

//String print info in mail
func (info *CreateSequenceInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "SequenceName:%s,Statement:%s,User:%s", info.SequenceName, info.Statement, info.User)
	return sb.String()
}

//AlterSequenceInfo info struct
type AlterSequenceInfo struct {
	SequenceName string
	Statement    string
	User         string
}

//String print info in mail
func (info *AlterSequenceInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "SequenceName:%s,Statement:%s,User:%s", info.SequenceName, info.Statement, info.User)
	return sb.String()
}

//DropSequenceInfo info struct
type DropSequenceInfo struct {
	SequenceName string
	Statement    string
	User         string
}

//String print info in mail
func (info *DropSequenceInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "SequenceName:%s,Statement:%s,User:%s", info.SequenceName, info.Statement, info.User)
	return sb.String()
}

//CreateViewInfo info struct
type CreateViewInfo struct {
	ViewName  string
	Statement string
	User      string
}

//String print info in mail
func (info *CreateViewInfo) String() string {
	var sb strings.Builder
	_, _ = fmt.Fprintf(&sb, "ViewName:%s,Statement:%s,User:%s", info.ViewName, info.Statement, info.User)
	return sb.String()
}

//DropViewInfo info struct
type DropViewInfo struct {
	ViewName            string
	Statement           string
	User                string
	CascadeDroppedViews []string
}

//String print info in mail
func (info *DropViewInfo) String() string {
	var sb strings.Builder
	var temp string
	var value string
	for _, value = range info.CascadeDroppedViews {
		temp = temp + " " + value
	}
	if temp != "" {
		_, _ = fmt.Fprintf(&sb, "ViewName:%s,Statement:%s,User:%s,CascadeDroppedViews:%v", info.ViewName, info.Statement, info.User, temp)
	} else {
		_, _ = fmt.Fprintf(&sb, "ViewName:%s,Statement:%s,User:%s", info.ViewName, info.Statement, info.User)
	}
	return sb.String()
}
