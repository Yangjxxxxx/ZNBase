package tree

import "github.com/znbasedb/znbase/pkg/roachpb"

// Location represents a Location statement.
type Location struct {
	Locate NameList
	Lease  NameList
}

// A LocationList is a list of identifiers.
type LocationList []*Location

// NewNameList converts a slice of string to the NameList type
func NewNameList(s []string) NameList {
	res := make([]Name, len(s))
	for i, c := range s {
		res[i] = Name(c)
	}
	return res
}

// NewLocation converts a *roachpb.LocationValue to the *Location type
func NewLocation(lv *roachpb.LocationValue) *Location {
	if lv == nil {
		return nil
	}
	return &Location{
		Locate: NewNameList(lv.Spaces),
		Lease:  NewNameList(lv.Leases),
	}
}

// ToValue converts a *Location to the *roachpb.LocationValue type
func (node *Location) ToValue() *roachpb.LocationValue {
	if node == nil {
		return nil
	}
	return &roachpb.LocationValue{
		Spaces: node.Locate.ToStrings(),
		Leases: node.Lease.ToStrings(),
	}
}

// Format implements the NodeFormatter interface.
func (node *Location) Format(ctx *FmtCtx) {
	if node == nil {
		return
	}
	if len(node.Locate) > 0 {
		ctx.WriteString(" LOCATE IN ")
		ctx.WriteString("(")
		node.Locate.Format(ctx)
		ctx.WriteString(")")
	}
	if len(node.Lease) > 0 {
		ctx.WriteString(" LEASE IN")
		ctx.WriteString(" (")
		node.Lease.Format(ctx)
		ctx.WriteString(")")
	}
	return
}
