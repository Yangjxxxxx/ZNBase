// Copyright 2020  The Cockroach Authors.

package sql

import (
	"context"

	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgnotice"
	"github.com/znbasedb/znbase/pkg/util/log"
)

// BufferClientNotice implements the tree.ClientNoticeSender interface.
func (p *planner) BufferClientNotice(ctx context.Context, notice pgnotice.Notice) {
	if log.V(2) {
		log.Infof(ctx, "buffered notice: %+v", notice)
	}
	noticeSeverity, ok := pgnotice.ParseDisplaySeverity(pgerror.GetSeverity(notice))
	if !ok {
		noticeSeverity = pgnotice.DisplaySeverityNotice
	}
	if p.noticeSender == nil ||
		noticeSeverity > p.SessionData().NoticeDisplaySeverity ||
		!NoticesEnabled.Get(&p.execCfg.Settings.SV) {
		// Notice cannot flow to the client - because of one of these conditions:
		// * there is no client
		// * the session's NoticeDisplaySeverity is higher than the severity of the notice.
		// * the notice protocol was disabled
		return
	}
	p.noticeSender.BufferNotice(notice)
}
