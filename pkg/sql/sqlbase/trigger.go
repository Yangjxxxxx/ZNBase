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

package sqlbase

import "github.com/znbasedb/znbase/pkg/sql/sem/tree"

// Trigger info
type Trigger struct {
	Tgrelid        tree.ID //-- id of relation table
	Tgname         string
	TgfuncName     string
	TgfuncID       uint16
	Tgtype         uint16
	Tgenable       bool
	Tgsconstraint  bool
	Tgconstrname   string
	Tgconstrrelid  tree.ID
	Tgdeferrable   bool
	Tginitdeferred bool
	Tgnargs        uint8
	Tgargs         []string
	Tgwhenexpr     string
	TgProHint      bool // 标记绑定的存储过程是否是创建触发器时生成
}

//Set trigger tree.datam
func (tg *Trigger) Set(idx uint32, dt tree.Datum) {
	switch idx {
	case 0:
		raw, ok := dt.(*tree.DInt)
		if ok {
			tg.Tgrelid = tree.ID(*raw)
		}
	case 1:
		raw, ok := dt.(*tree.DString)
		if ok {
			tg.Tgname = string(*raw)
		}
	case 2:
		raw, ok := dt.(*tree.DInt)
		if ok {
			tg.TgfuncID = uint16(*raw)
		}
	case 3:
		raw, ok := dt.(*tree.DInt)
		if ok {
			tg.Tgtype = uint16(*raw)
		}
	case 4:
		raw, ok := dt.(*tree.DBool)
		if ok {
			tg.Tgenable = bool(*raw)
		}
	case 5:
		raw, ok := dt.(*tree.DBool)
		if ok {
			tg.Tgsconstraint = bool(*raw)
		}
	case 6:
		raw, ok := dt.(*tree.DString)
		if ok {
			tg.Tgconstrname = string(*raw)
		}
	case 7:
		raw, ok := dt.(*tree.DInt)
		if ok {
			tg.Tgconstrrelid = tree.ID(*raw)
		}
	case 8:
		raw, ok := dt.(*tree.DBool)
		if ok {
			tg.Tgdeferrable = bool(*raw)
		}
	case 9:
		raw, ok := dt.(*tree.DBool)
		if ok {
			tg.Tginitdeferred = bool(*raw)
		}
	case 10:
		raw, ok := dt.(*tree.DInt)
		if ok {
			tg.Tgnargs = uint8(*raw)
		}
	case 11:
		raw, ok := dt.(*tree.DArray)
		if ok {
			str := make([]string, 0)
			for _, k := range raw.Array {
				if p, okk := k.(*tree.DString); okk {
					str = append(str, string(*p))
				}
			}
			tg.Tgargs = str
		}
	case 12:
		raw, ok := dt.(*tree.DBytes)
		if ok {
			tg.Tgwhenexpr = string(*raw)
		}
	case 13:
		raw, ok := dt.(*tree.DBool)
		if ok {
			tg.TgProHint = bool(*raw)
		}
	default:

	}
}

//IsType used to judge trigger type
func (tg *Trigger) IsType(eventType uint32, runType uint32, levelType uint32) bool {
	return uint32(tg.Tgtype)&(eventType|runType|levelType) == eventType|runType|levelType
}

//IsAfterStmt used to judge trigger if AFTER
func (tg *Trigger) IsAfterStmt(eventType, levelType uint32) bool {
	return uint32(tg.Tgtype)&(eventType|levelType) == eventType|levelType
}

// TriggerDesc of table
type TriggerDesc struct {
	Triggers                  []Trigger /* array of Trigger structs */
	trigInsertBeforeRow       bool
	trigInsertAfterRow        bool
	trigInsertInsteadRow      bool
	trigInsertBeforeStatement bool
	trigInsertAfterStatement  bool
	trigUpdateBeforeRow       bool
	trigUpdateAfterRow        bool
	trigUpdateInsteadRow      bool
	trigUpdateBeforeStatement bool
	trigUpdateAfterStatement  bool
	trigDeleteBeforeRow       bool
	trigDeleteAfterRow        bool
	trigDeleteInsteadRow      bool
	trigDeleteBeforeStatement bool
	trigDeleteAfterStatement  bool
	/* there are no row-level truncate triggers */
	trigTruncateBeforeStatement bool
	trigTruncateAfterStatement  bool
	/* Is there at least one trigger specifying each transition relation? */
	trigInsertNewTable bool
	trigUpdateOldTable bool
	trigUpdateNewTable bool
	trigDeleteOldTable bool
}
