package event

import (
	"context"
	"fmt"

	"github.com/znbasedb/znbase/pkg/mail"
)

// auditAction interface.
type auditAction interface {
	action(func())
}

// mailAction struct
type mailAction struct {
	mail *mail.Server
}

func (ma *mailAction) action(msg func() (context.Context, string, string)) {
	if err := ma.mail.Send(msg()); err != nil {
		fmt.Printf("%s", err)
	}
}
