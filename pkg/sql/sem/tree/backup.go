// Copyright 2016  The Cockroach Authors.
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

// Backup represents a BACKUP statement.
type Backup struct {
	Targets         TargetList
	To              Expr
	IncrementalFrom Exprs
	AsOf            AsOfClause
	Options         KVOptions
}

var _ Statement = &Backup{}

// Format implements the NodeFormatter interface.
func (n *Backup) Format(ctx *FmtCtx) {
	ctx.WriteString("BACKUP ")
	ctx.FormatNode(&n.Targets)
	ctx.WriteString(" TO ")
	ctx.FormatNode(n.To)
	if n.AsOf.Expr != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&n.AsOf)
	}
	if n.IncrementalFrom != nil {
		ctx.WriteString(" INCREMENTAL FROM ")
		ctx.FormatNode(&n.IncrementalFrom)
	}
	if n.Options != nil {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&n.Options)
	}
}

// KVOption is a key-value option.
type KVOption struct {
	Key   Name
	Value Expr
}

// KVOptions is a list of KVOptions.
type KVOptions []KVOption

//FindByName 提供一个可以通过名称判断是否具有相关options的方法
func (o KVOptions) FindByName(name string) bool {
	for _, v := range o {
		if string(v.Key) == name {
			return true
		}
	}
	return false
}

// Format implements the NodeFormatter interface.
func (o *KVOptions) Format(ctx *FmtCtx) {
	for i := range *o {
		n := &(*o)[i]
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&n.Key)
		if n.Value != nil {
			ctx.WriteString(` = `)
			ctx.FormatNode(n.Value)
		}
	}
}
