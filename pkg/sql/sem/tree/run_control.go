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

import "fmt"

// ControlJobs represents a PAUSE/RESUME/CANCEL JOBS statement.
type ControlJobs struct {
	Jobs    *Select
	Command JobCommand
}

// JobCommand determines which type of action to effect on the selected job(s).
type JobCommand int

// JobCommand values
const (
	PauseJob JobCommand = iota
	CancelJob
	ResumeJob
)

// JobCommandToStatement translates a job command integer to a statement prefix.
var JobCommandToStatement = map[JobCommand]string{
	PauseJob:  "PAUSE",
	CancelJob: "CANCEL",
	ResumeJob: "RESUME",
}

// Format implements the NodeFormatter interface.
func (c *ControlJobs) Format(ctx *FmtCtx) {
	ctx.WriteString(JobCommandToStatement[c.Command])
	ctx.WriteString(" JOBS ")
	ctx.FormatNode(c.Jobs)
}

// CancelQueries represents a CANCEL QUERIES statement.
type CancelQueries struct {
	Queries  *Select
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *CancelQueries) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL QUERIES ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(n.Queries)
}

// QueryLockstat represents a QUERY LOCKSTAT statement.
type QueryLockstat struct {
	Txnid *Select
}

// Format implements the NodeFormatter interface.
func (q *QueryLockstat) Format(ctx *FmtCtx) {
	_, _ = ctx.WriteString("QUERY LOCKSTAT ")
	ctx.FormatNode(q.Txnid)
}

// CancelSessions represents a CANCEL SESSIONS statement.
type CancelSessions struct {
	Sessions *Select
	IfExists bool
}

// Format implements the NodeFormatter interface.
func (n *CancelSessions) Format(ctx *FmtCtx) {
	ctx.WriteString("CANCEL SESSIONS ")
	if n.IfExists {
		ctx.WriteString("IF EXISTS ")
	}
	ctx.FormatNode(n.Sessions)
}

// ScheduleCommand determines which type of action to effect on the selected job(s).
type ScheduleCommand int

// ScheduleCommand values
const (
	PauseSchedule ScheduleCommand = iota
	ResumeSchedule
	DropSchedule
)

func (c ScheduleCommand) String() string {
	switch c {
	case PauseSchedule:
		return "PAUSE"
	case ResumeSchedule:
		return "RESUME"
	case DropSchedule:
		return "DROP"
	default:
		panic("unhandled schedule command")
	}
}

// ControlSchedules represents PAUSE/RESUME SCHEDULE statement.
type ControlSchedules struct {
	Schedules *Select
	Command   ScheduleCommand
}

// StatAbbr implements the StatAbbr interface.
func (c *ControlSchedules) StatAbbr() string {
	return "ControlSchedules"
}

// StatObj implements the StatObj interface.
func (c *ControlSchedules) StatObj() string {
	return ""
}

var _ Statement = &ControlSchedules{}

// Format implements NodeFormatter interface
func (c *ControlSchedules) Format(ctx *FmtCtx) {
	fmt.Fprintf(ctx, "%s SCHEDULES ", c.Command)
	c.Schedules.Format(ctx)
}
