// Copyright 2018  The Cockroach Authors.
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

import (
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/util/pretty"
)

// This file contains methods that convert statements to pretty Docs (a tree
// structure that can be pretty printed at a specific line width). Nodes
// implement the docer interface to allow this conversion. In general,
// a node implements doc by copying its Format method and returning a Doc
// structure instead of writing to a buffer. Some guidelines are below.
//
// Nodes should not precede themselves with a space. Instead, the parent
// structure should correctly add spaces when needed.
//
// nestName should be used for most `KEYWORD <expr>` constructs.
//
// Nodes that never need to line break or for which the Format method already
// produces a compact representation should not implement doc, but instead
// rely on the default fallback that uses .Format. Examples include datums
// and constants.

// PrettyCfg holds configuration for pretty printing statements.
type PrettyCfg struct {
	// LineWidth is the desired maximum line width.
	LineWidth int
	// TabWidth is the amount of spaces to use for tabs when UseTabs is
	// false.
	TabWidth int
	// Align, when set to another value than PrettyNoAlign, uses
	// alignment for some constructs as a first choice. If not set or if
	// the line width is insufficient, nesting is used instead.
	Align PrettyAlignMode
	// UseTabs indicates whether to use tab chars to signal indentation.
	UseTabs bool
	// Simplify, when set, removes extraneous parentheses.
	Simplify bool
	// Case, if set, transforms case-insensitive strings (like SQL keywords).
	Case func(string) string
}

// DefaultPrettyCfg returns a PrettyCfg with the default
// configuration.
func DefaultPrettyCfg() PrettyCfg {
	return PrettyCfg{
		LineWidth: 100,
		Simplify:  true,
		TabWidth:  4,
		UseTabs:   true,
		Align:     PrettyNoAlign, // TODO(knz): I really want this to be AlignAndDeindent
	}
}

// PrettyAlignMode directs which alignment mode to use.
//
// TODO(knz/mjibson): this variety of options currently exists so as
// to enable comparisons and gauging individual preferences. We should
// aim to remove some or all of these options in the future.
type PrettyAlignMode int

const (
	// PrettyNoAlign disables alignment.
	PrettyNoAlign PrettyAlignMode = 0
	// PrettyAlignOnly aligns sub-clauses only and preserves the
	// hierarchy of logical operators.
	PrettyAlignOnly = 1
	// PrettyAlignAndDeindent does the work of PrettyAlignOnly and also
	// de-indents AND and OR operators.
	PrettyAlignAndDeindent = 2
	// PrettyAlignAndExtraIndent does the work of PrettyAlignOnly and
	// also extra indents the operands of AND and OR operators so
	// that they appear aligned but also indented.
	PrettyAlignAndExtraIndent = 3
)

// prettyKeywordWithText returns a pretty.Keyword with left and/or right
// sides concat'd as a pretty.Text.
func prettyKeywordWithText(left, keyword, right string) pretty.Doc {
	doc := pretty.Keyword(keyword)
	if left != "" {
		doc = pretty.Concat(pretty.Text(left), doc)
	}
	if right != "" {
		doc = pretty.Concat(doc, pretty.Text(right))
	}
	return doc
}

func prettyBracketKeyword(
	leftKeyword, leftParen string, inner pretty.Doc, rightParen, rightKeyword string,
) pretty.Doc {
	var left, right pretty.Doc
	if leftKeyword != "" {
		left = prettyKeywordWithText("", leftKeyword, leftParen)
	} else {
		left = pretty.Text(leftParen)
	}
	if rightKeyword != "" {
		right = prettyKeywordWithText(rightParen, rightKeyword, "")
	} else {
		right = pretty.Text(rightParen)
	}
	return pretty.BracketDoc(left, inner, right)
}

// Pretty pretty prints stmt with default options.
func Pretty(stmt NodeFormatter) string {
	cfg := DefaultPrettyCfg()
	return cfg.Pretty(stmt)
}
func (node *LikeTableDef) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Keyword("LIKE")
	d = pretty.ConcatSpace(d, p.Doc(&node.Name))
	for _, opt := range node.Options {
		word := "INCLUDING"
		if opt.Excluded {
			word = "EXCLUDING"
		}
		d = pretty.ConcatSpace(d, pretty.Keyword(word))
		d = pretty.ConcatSpace(d, pretty.Keyword(opt.Opt.String()))
	}
	return d
}

// Pretty pretty prints stmt with specified options.
func (p *PrettyCfg) Pretty(stmt NodeFormatter) string {
	doc := p.Doc(stmt)
	return pretty.Pretty(doc, p.LineWidth, p.UseTabs, p.TabWidth, p.Case)
}

// Doc converts f (generally a Statement) to a pretty.Doc. If f does not have a
// native conversion, its .Format representation is used as a simple Text Doc.
func (p *PrettyCfg) Doc(f NodeFormatter) pretty.Doc {
	if f, ok := f.(docer); ok {
		doc := f.doc(p)
		return doc
	}
	return p.docAsString(f)
}

func (p *PrettyCfg) docAsString(f NodeFormatter) pretty.Doc {
	const prettyFlags = FmtShowPasswords | FmtParsable
	return pretty.Text(AsStringWithFlags(f, prettyFlags))
}

func (p *PrettyCfg) nestUnder(a, b pretty.Doc) pretty.Doc {
	if p.Align != PrettyNoAlign {
		return pretty.AlignUnder(a, b)
	}
	return pretty.NestUnder(a, b)
}

func (p *PrettyCfg) rlTable(rows ...pretty.RLTableRow) pretty.Doc {
	return pretty.RLTable(p.Align != PrettyNoAlign, pretty.Keyword, rows...)
}

func (p *PrettyCfg) row(lbl string, d pretty.Doc) pretty.RLTableRow {
	return pretty.RLTableRow{Label: lbl, Doc: d}
}

var emptyRow = pretty.RLTableRow{}

func (p *PrettyCfg) unrow(r pretty.RLTableRow) pretty.Doc {
	if r.Doc == nil {
		return pretty.Nil
	}
	if r.Label == "" {
		return r.Doc
	}
	return p.nestUnder(pretty.Text(r.Label), r.Doc)
}

func (p *PrettyCfg) joinNestedOuter(lbl string, d ...pretty.Doc) pretty.Doc {
	if len(d) == 0 {
		return pretty.Nil
	}
	switch p.Align {
	case PrettyAlignAndDeindent:
		return pretty.JoinNestedOuter(lbl, pretty.Keyword, d...)
	case PrettyAlignAndExtraIndent:
		items := make([]pretty.RLTableRow, len(d))
		for i, dd := range d {
			if i > 0 {
				items[i].Label = lbl
			}
			items[i].Doc = dd
		}
		return pretty.RLTable(true, pretty.Keyword, items...)
	default:
		return pretty.JoinNestedRight(pretty.Keyword(lbl), d...)
	}
}

// docer is implemented by nodes that can convert themselves into
// pretty.Docs. If nodes cannot, node.Format is used instead as a Text Doc.
type docer interface {
	doc(*PrettyCfg) pretty.Doc
}

// tableDocer is implemented by nodes that can convert themselves
// into []pretty.RLTableRow, i.e. a table.
type tableDocer interface {
	docTable(*PrettyCfg) []pretty.RLTableRow
}

func (node SelectExprs) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		d[i] = e.doc(p)
	}
	return pretty.Join(",", d...)
}

func (node SelectExpr) doc(p *PrettyCfg) pretty.Doc {
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	d := p.Doc(e)
	if node.As != "" {
		d = p.nestUnder(
			d,
			pretty.Concat(prettyKeywordWithText("", "AS", " "), p.Doc(&node.As)),
		)
	}
	return d
}

func (node TableExprs) doc(p *PrettyCfg) pretty.Doc {
	if len(node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(node))
	for i, e := range node {
		if p.Simplify {
			e = StripTableParens(e)
		}
		d[i] = p.Doc(e)
	}
	return pretty.Join(",", d...)
}

func (node *Where) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *Where) docRow(p *PrettyCfg) pretty.RLTableRow {
	if node == nil {
		return emptyRow
	}
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	return p.row(node.Type, p.Doc(e))
}

func (node *GroupBy) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *GroupBy) docRow(p *PrettyCfg) pretty.RLTableRow {
	if len(*node) == 0 {
		return emptyRow
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		// Beware! The GROUP BY items should never be simplified by
		// stripping parentheses, because parentheses there are
		// semantically important.
		d[i] = p.Doc(e)
	}
	return p.row("GROUP BY", pretty.Join(",", d...))
}

// flattenOp populates a slice with all the leaves operands of an expression
// tree where all the nodes satisfy the given predicate.
func (p *PrettyCfg) flattenOp(
	e Expr,
	pred func(e Expr, recurse func(e Expr)) bool,
	formatOperand func(e Expr) pretty.Doc,
	in []pretty.Doc,
) []pretty.Doc {
	if ok := pred(e, func(sub Expr) {
		in = p.flattenOp(sub, pred, formatOperand, in)
	}); ok {
		return in
	}
	return append(in, formatOperand(e))
}

func (p *PrettyCfg) peelAndOrOperand(e Expr) Expr {
	if !p.Simplify {
		return e
	}
	stripped := StripParens(e)
	switch stripped.(type) {
	case *BinaryExpr, *ComparisonExpr, *RangeCond, *FuncExpr, *IndirectionExpr,
		*UnaryExpr, *AnnotateTypeExpr, *CastExpr, *ColumnItem, *UnresolvedName:
		// All these expressions have higher precedence than binary
		// expressions.
		return stripped
	}
	// Everything else - we don't know. Be conservative and keep the
	// original form.
	return e
}

func (node *AndExpr) doc(p *PrettyCfg) pretty.Doc {
	pred := func(e Expr, recurse func(e Expr)) bool {
		if a, ok := e.(*AndExpr); ok {
			recurse(a.Left)
			recurse(a.Right)
			return true
		}
		return false
	}
	formatOperand := func(e Expr) pretty.Doc {
		return p.Doc(p.peelAndOrOperand(e))
	}
	operands := p.flattenOp(node.Left, pred, formatOperand, nil)
	operands = p.flattenOp(node.Right, pred, formatOperand, operands)
	return p.joinNestedOuter("AND", operands...)
}

func (node *OrExpr) doc(p *PrettyCfg) pretty.Doc {
	pred := func(e Expr, recurse func(e Expr)) bool {
		if a, ok := e.(*OrExpr); ok {
			recurse(a.Left)
			recurse(a.Right)
			return true
		}
		return false
	}
	formatOperand := func(e Expr) pretty.Doc {
		return p.Doc(p.peelAndOrOperand(e))
	}
	operands := p.flattenOp(node.Left, pred, formatOperand, nil)
	operands = p.flattenOp(node.Right, pred, formatOperand, operands)
	return p.joinNestedOuter("OR", operands...)
}

func (node *Exprs) doc(p *PrettyCfg) pretty.Doc {
	if node == nil || len(*node) == 0 {
		return pretty.Nil
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		if p.Simplify {
			e = StripParens(e)
		}
		d[i] = p.Doc(e)
	}
	return pretty.Join(",", d...)
}

// peelBinaryOperand conditionally (p.Simplify) removes the
// parentheses around an expression. The parentheses are always
// removed in the following conditions:
// - if the operand is a unary operator (these are always
//   of higher precedence): "(-a) * b" -> "-a * b"
// - if the operand is a binary operator and its precedence
//   is guaranteed to be higher: "(a * b) + c" -> "a * b + c"
//
// Additionally, iff sameLevel is set, then parentheses are removed
// around any binary operator that has the same precedence level as
// the parent.
// sameLevel can be set:
//
// - for the left operand of all binary expressions, because
//   (in pg SQL) all binary expressions are left-associative.
//   This rewrites e.g. "(a + b) - c" -> "a + b - c"
//   and "(a - b) + c" -> "a - b + c"
// - for the right operand when the parent operator is known
//   to be fully associative, e.g.
//   "a + (b - c)" -> "a + b - c" because "+" is fully assoc,
//   but "a - (b + c)" cannot be simplified because "-" is not fully associative.
//
func (p *PrettyCfg) peelBinaryOperand(e Expr, sameLevel bool, parenPrio int) Expr {
	if !p.Simplify {
		return e
	}
	stripped := StripParens(e)
	switch te := stripped.(type) {
	case *BinaryExpr:
		childPrio := binaryOpPrio[te.Operator]
		if childPrio < parenPrio || (sameLevel && childPrio == parenPrio) {
			return stripped
		}
	case *FuncExpr, *UnaryExpr, *AnnotateTypeExpr, *IndirectionExpr,
		*CastExpr, *ColumnItem, *UnresolvedName:
		// All these expressions have higher precedence than binary expressions.
		return stripped
	}
	// Everything else - we don't know. Be conservative and keep the
	// original form.
	return e
}

func (node *BinaryExpr) doc(p *PrettyCfg) pretty.Doc {
	// All the binary operators are at least left-associative.
	// So we can always simplify "(a OP b) OP c" to "a OP b OP c".
	parenPrio := binaryOpPrio[node.Operator]
	leftOperand := p.peelBinaryOperand(node.Left, true /*sameLevel*/, parenPrio)
	// If the binary operator is also fully associative,
	// we can also simplify "a OP (b OP c)" to "a OP b OP c".
	opFullyAssoc := binaryOpFullyAssoc[node.Operator]
	rightOperand := p.peelBinaryOperand(node.Right, opFullyAssoc, parenPrio)

	opDoc := pretty.Text(node.Operator.String())
	var res pretty.Doc
	if !node.Operator.isPadded() {
		res = pretty.JoinDoc(opDoc, p.Doc(leftOperand), p.Doc(rightOperand))
	} else {
		pred := func(e Expr, recurse func(e Expr)) bool {
			if b, ok := e.(*BinaryExpr); ok && b.Operator == node.Operator {
				leftSubOperand := p.peelBinaryOperand(b.Left, true /*sameLevel*/, parenPrio)
				rightSubOperand := p.peelBinaryOperand(b.Right, opFullyAssoc, parenPrio)
				recurse(leftSubOperand)
				recurse(rightSubOperand)
				return true
			}
			return false
		}
		formatOperand := func(e Expr) pretty.Doc {
			return p.Doc(e)
		}
		operands := p.flattenOp(leftOperand, pred, formatOperand, nil)
		operands = p.flattenOp(rightOperand, pred, formatOperand, operands)
		res = pretty.JoinNestedRight(
			opDoc, operands...)
	}
	return pretty.Group(res)
}

func (node *ParenExpr) doc(p *PrettyCfg) pretty.Doc {
	return pretty.Bracket("(", p.Doc(node.Expr), ")")
}

func (n *ParenSelect) doc(p *PrettyCfg) pretty.Doc {
	return pretty.Bracket("(", p.Doc(n.Select), ")")
}

func (node *ParenTableExpr) doc(p *PrettyCfg) pretty.Doc {
	return pretty.Bracket("(", p.Doc(node.Expr), ")")
}

func (node *Limit) doc(p *PrettyCfg) pretty.Doc {
	res := pretty.Nil
	for i, r := range node.docTable(p) {
		if r.Doc != nil {
			if i > 0 {
				res = pretty.Concat(res, pretty.Line)
			}
			res = pretty.Concat(res, p.nestUnder(pretty.Text(r.Label), r.Doc))
		}
	}
	return res
}

func (node *Limit) docTable(p *PrettyCfg) []pretty.RLTableRow {
	if node == nil {
		return nil
	}
	res := make([]pretty.RLTableRow, 0, 2)
	if node.Count != nil {
		e := node.Count
		if p.Simplify {
			e = StripParens(e)
		}
		res = append(res, p.row("LIMIT", p.Doc(e)))
	}
	if node.Offset != nil {
		e := node.Offset
		if p.Simplify {
			e = StripParens(e)
		}
		res = append(res, p.row("OFFSET", p.Doc(e)))
	}
	return res
}

func (node *OrderBy) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *OrderBy) docRow(p *PrettyCfg) pretty.RLTableRow {
	if node == nil || len(*node) == 0 {
		return emptyRow
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		// Beware! The ORDER BY items should never be simplified,
		// because parentheses there are semantically important.
		d[i] = p.Doc(e)
	}
	return p.row("ORDER BY", pretty.Join(",", d...))
}

func (n *Select) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(n.docTable(p)...)
}

func (n *Select) docTable(p *PrettyCfg) []pretty.RLTableRow {
	items := make([]pretty.RLTableRow, 0, 9)
	items = append(items, n.With.docRow(p))
	if s, ok := n.Select.(tableDocer); ok {
		items = append(items, s.docTable(p)...)
	} else {
		items = append(items, p.row("", p.Doc(n.Select)))
	}
	items = append(items, n.OrderBy.docRow(p))
	items = append(items, n.Limit.docTable(p)...)
	items = append(items, n.ForLocked.docTable(p)...)
	return items
}

func (node *LockingClause) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(node.docTable(p)...)
}

func (node *LockingClause) docTable(p *PrettyCfg) []pretty.RLTableRow {
	items := make([]pretty.RLTableRow, len(*node))
	for i, n := range *node {
		items[i] = p.row("", p.Doc(n))
	}
	return items
}

func (f *LockingItem) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(f.docTable(p)...)
}

func (f *LockingItem) docTable(p *PrettyCfg) []pretty.RLTableRow {
	if f.Strength == ForNone {
		return nil
	}
	items := make([]pretty.RLTableRow, 0, 3)
	items = append(items, f.Strength.docTable(p)...)
	if len(f.Targets) > 0 {
		items = append(items, p.row("OF", p.Doc(&f.Targets)))
	}
	items = append(items, f.WaitPolicy.docTable(p)...)
	return items
}

func (f LockingStrength) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(f.docTable(p)...)
}

func (f LockingStrength) docTable(p *PrettyCfg) []pretty.RLTableRow {
	str := f.String()
	if str == "" {
		return nil
	}
	return []pretty.RLTableRow{p.row("", pretty.Keyword(str))}
}

func (f LockingWaitPolicy) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(f.docTable(p)...)
}

func (f LockingWaitPolicy) docTable(p *PrettyCfg) []pretty.RLTableRow {
	str := f.String()
	if str == "" {
		return nil
	}
	return []pretty.RLTableRow{p.row("", pretty.Keyword(str))}
}

func (n *SelectClause) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(n.docTable(p)...)
}

func (n *SelectClause) docTable(p *PrettyCfg) []pretty.RLTableRow {
	if n.TableSelect {
		return []pretty.RLTableRow{p.row("TABLE", p.Doc(n.From.Tables[0]))}
	}
	exprs := n.Exprs.doc(p)
	if n.Distinct {
		if n.DistinctOn != nil {
			exprs = pretty.ConcatLine(p.Doc(&n.DistinctOn), exprs)
		} else {
			exprs = pretty.ConcatLine(pretty.Keyword("DISTINCT"), exprs)
		}
	}
	return []pretty.RLTableRow{
		p.row("SELECT", exprs),
		n.From.docRow(p),
		n.Where.docRow(p),
		n.GroupBy.docRow(p),
		n.Having.docRow(p),
		n.Window.docRow(p),
	}
}

func (node *From) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *From) docRow(p *PrettyCfg) pretty.RLTableRow {
	if node == nil || len(node.Tables) == 0 {
		return emptyRow
	}
	d := node.Tables.doc(p)
	if node.AsOf.Expr != nil {
		d = p.nestUnder(
			d,
			p.Doc(&node.AsOf),
		)
	}
	return p.row("FROM", d)
}

func (node *Window) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *Window) docRow(p *PrettyCfg) pretty.RLTableRow {
	if node == nil || len(*node) == 0 {
		return emptyRow
	}
	d := make([]pretty.Doc, len(*node))
	for i, e := range *node {
		d[i] = pretty.Fold(pretty.Concat,
			pretty.Text(e.Name.String()),
			prettyKeywordWithText(" ", "AS", " "),
			p.Doc(e),
		)
	}
	return p.row("WINDOW", pretty.Join(",", d...))
}

func (node *With) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *With) docRow(p *PrettyCfg) pretty.RLTableRow {
	if node == nil {
		return emptyRow
	}
	d := make([]pretty.Doc, len(node.CTEList))
	for i, cte := range node.CTEList {
		d[i] = p.nestUnder(
			p.Doc(&cte.Name),
			prettyBracketKeyword("AS", " (", p.Doc(cte.Stmt), ")", ""),
		)
	}
	kw := "WITH"
	if node.Recursive {
		kw = "WITH RECURSIVE"
	}
	return p.row(kw, pretty.Join(",", d...))
}

func (node *Subquery) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Text("<unknown>")
	if node.Select != nil {
		d = p.Doc(node.Select)
	}
	if node.Exists {
		d = pretty.Concat(
			pretty.Keyword("EXISTS"),
			d,
		)
	}
	return d
}

func (node *AliasedTableExpr) doc(p *PrettyCfg) pretty.Doc {
	d := p.Doc(node.Expr)
	if node.IndexFlags != nil {
		d = pretty.Concat(
			d,
			p.Doc(node.IndexFlags),
		)
	}
	if node.Ordinality {
		d = pretty.Concat(
			d,
			prettyKeywordWithText(" ", "WITH ORDINALITY", ""),
		)
	}
	if node.As.Alias != "" {
		d = p.nestUnder(
			d,
			pretty.Concat(
				prettyKeywordWithText("", "AS", " "),
				p.Doc(&node.As),
			),
		)
	}
	return d
}

func (node *FuncExpr) doc(p *PrettyCfg) pretty.Doc {
	d := p.Doc(&node.Func)

	if len(node.Exprs) > 0 {
		args := node.Exprs.doc(p)
		if node.Type != 0 {
			args = pretty.ConcatLine(
				pretty.Text(funcTypeName[node.Type]),
				args,
			)
		}
		d = pretty.Concat(d, pretty.Bracket("(", args, ")"))
	} else {
		d = pretty.Concat(d, pretty.Text("()"))
	}
	if node.Filter != nil {
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			pretty.Keyword("FILTER"),
			pretty.Bracket("(",
				p.nestUnder(pretty.Keyword("WHERE"), p.Doc(node.Filter)),
				")"))
	}
	if window := node.WindowDef; window != nil {
		var over pretty.Doc
		if window.Name != "" {
			over = p.Doc(&window.Name)
		} else {
			over = p.Doc(window)
		}
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			pretty.Keyword("OVER"),
			over,
		)
	}
	return d
}

func (node *WindowDef) doc(p *PrettyCfg) pretty.Doc {
	rows := make([]pretty.RLTableRow, 0, 4)
	if node.RefName != "" {
		rows = append(rows, p.row("", p.Doc(&node.RefName)))
	}
	if len(node.Partitions) > 0 {
		rows = append(rows, p.row("PARTITION BY", p.Doc(&node.Partitions)))
	}
	if len(node.OrderBy) > 0 {
		rows = append(rows, node.OrderBy.docRow(p))
	}
	if node.Frame != nil {
		rows = append(rows, node.Frame.docRow(p))
	}
	if len(rows) == 0 {
		return pretty.Text("()")
	}
	return pretty.Bracket("(", p.rlTable(rows...), ")")
}

func (wf *WindowFrame) docRow(p *PrettyCfg) pretty.RLTableRow {
	kw := "RANGE"
	if wf.Mode == ROWS {
		kw = "ROWS"
	} else if wf.Mode == GROUPS {
		kw = "GROUPS"
	}
	d := p.Doc(wf.Bounds.StartBound)
	if wf.Bounds.EndBound != nil {
		d = p.rlTable(
			p.row("BETWEEN", d),
			p.row("AND", p.Doc(wf.Bounds.EndBound)),
		)
	}
	return p.row(kw, d)
}

func (node *WindowFrameBound) doc(p *PrettyCfg) pretty.Doc {
	switch node.BoundType {
	case UnboundedPreceding:
		return pretty.Keyword("UNBOUNDED PRECEDING")
	case OffsetPreceding:
		return pretty.ConcatSpace(p.Doc(node.OffsetExpr), pretty.Keyword("PRECEDING"))
	case CurrentRow:
		return pretty.Keyword("CURRENT ROW")
	case OffsetFollowing:
		return pretty.ConcatSpace(p.Doc(node.OffsetExpr), pretty.Keyword("FOLLOWING"))
	case UnboundedFollowing:
		return pretty.Keyword("UNBOUNDED FOLLOWING")
	default:
		panic(fmt.Sprintf("unexpected type %d", node.BoundType))
	}
}

func (p *PrettyCfg) peelCompOperand(e Expr) Expr {
	if !p.Simplify {
		return e
	}
	stripped := StripParens(e)
	switch stripped.(type) {
	case *FuncExpr, *IndirectionExpr, *UnaryExpr,
		*AnnotateTypeExpr, *CastExpr, *ColumnItem, *UnresolvedName:
		return stripped
	}
	return e
}

func (node *ComparisonExpr) doc(p *PrettyCfg) pretty.Doc {
	opStr := node.Operator.String()
	if node.Operator == IsDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS NOT"
	} else if node.Operator == IsNotDistinctFrom && (node.Right == DNull || node.Right == DBoolTrue || node.Right == DBoolFalse) {
		opStr = "IS"
	}
	opDoc := pretty.Keyword(opStr)
	if node.Operator.hasSubOperator() {
		opDoc = pretty.ConcatSpace(pretty.Text(node.SubOperator.String()), opDoc)
	}
	return pretty.Group(
		pretty.JoinNestedRight(
			opDoc,
			p.Doc(p.peelCompOperand(node.Left)),
			p.Doc(p.peelCompOperand(node.Right))))
}

func (node *AliasClause) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Text(node.Alias.String())
	if len(node.Cols) != 0 {
		d = p.nestUnder(d, pretty.Bracket("(", p.Doc(&node.Cols), ")"))
	}
	return d
}

func (node *JoinTableExpr) doc(p *PrettyCfg) pretty.Doc {
	d := []pretty.Doc{p.Doc(node.Left)}
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax:
		//   "<a> NATURAL <join_type> [<join_hint>] <b>"
		j := p.Doc(node.Cond)
		if node.JoinType != "" {
			j = pretty.ConcatSpace(j, pretty.Text(node.JoinType))
			if node.Hint != "" {
				j = pretty.ConcatSpace(j, pretty.Text(node.Hint))
			}
		}
		j = pretty.ConcatSpace(j, pretty.Text("JOIN"))
		d = append(d, p.nestUnder(j, p.Doc(node.Right)))
	} else {
		// General syntax: "<a> <join_type> [<join_hint>] JOIN <b> <condition>"
		var j pretty.Doc
		if node.JoinType != "" {
			j = pretty.Text(node.JoinType)
			if node.Hint != "" {
				j = pretty.ConcatSpace(j, pretty.Text(node.Hint))
			}
			j = pretty.ConcatSpace(j, pretty.Text("JOIN"))
		} else {
			j = pretty.Text("JOIN")
		}

		operand := []pretty.Doc{p.nestUnder(j, p.Doc(node.Right))}
		if node.Cond != nil {
			operand = append(operand, p.Doc(node.Cond))
		}

		d = append(d, pretty.Group(pretty.Fold(pretty.ConcatLine, operand...)))
	}
	return pretty.Stack(d...)
}

func (node *OnJoinCond) doc(p *PrettyCfg) pretty.Doc {
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	return p.nestUnder(pretty.Keyword("ON"), p.Doc(e))
}

func (node *Insert) doc(p *PrettyCfg) pretty.Doc {
	if node.OnConflict.IsMergeStmt() {
		return p.docAsString(node)
	}
	items := make([]pretty.RLTableRow, 0, 8)
	items = append(items, node.With.docRow(p))
	kw := "INSERT"
	if node.OnConflict.IsUpsertAlias() {
		kw = "UPSERT"
	}
	items = append(items, p.row(kw, pretty.Nil))

	into := p.Doc(node.Table)
	if node.Columns != nil {
		into = p.nestUnder(into, pretty.Bracket("(", p.Doc(&node.Columns), ")"))
	}
	items = append(items, p.row("INTO", into))

	if node.DefaultValues() {
		items = append(items, p.row("", pretty.Keyword("DEFAULT VALUES")))
	} else {
		items = append(items, node.Rows.docTable(p)...)
	}

	if node.OnConflict != nil && !node.OnConflict.IsUpsertAlias() {
		cond := pretty.Nil
		if len(node.OnConflict.Columns) > 0 {
			cond = pretty.Bracket("(", p.Doc(&node.OnConflict.Columns), ")")
		}
		items = append(items, p.row("ON CONFLICT", cond))

		if node.OnConflict.DoNothing {
			items = append(items, p.row("DO", pretty.Keyword("NOTHING")))
		} else {
			items = append(items, p.row("DO",
				p.nestUnder(pretty.Keyword("UPDATE SET"), p.Doc(&node.OnConflict.Exprs))))
			if node.OnConflict.Where != nil {
				items = append(items, node.OnConflict.Where.docRow(p))
			}
		}
	}

	items = append(items, p.docReturning(node.Returning))
	return p.rlTable(items...)

}

func (node *NameList) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(&n)
	}
	return pretty.Join(",", d...)
}

func (node *CastExpr) doc(p *PrettyCfg) pretty.Doc {
	typ := pretty.Text(coltypes.ColTypeAsString(node.Type))

	switch node.SyntaxMode {
	case CastPrepend:
		// This is a special case for things like INTERVAL '1s'. These only work
		// with string constats; if the underlying expression was changed, we fall
		// back to the short syntax.
		if _, ok := node.Expr.(*StrVal); ok {
			return pretty.Fold(pretty.Concat,
				typ,
				pretty.Text(" "),
				p.Doc(node.Expr),
			)
		}
		fallthrough
	case CastShort:
		return pretty.Fold(pretty.Concat,
			p.exprDocWithParen(node.Expr),
			pretty.Text("::"),
			typ,
		)
	default:
		t, isCollatedString := node.Type.(*coltypes.TCollatedString)
		if isCollatedString {
			typ = pretty.Text(coltypes.String.String())
		}
		ret := pretty.Fold(pretty.Concat,
			pretty.Keyword("CAST"),
			pretty.Bracket(
				"(",
				p.nestUnder(
					p.Doc(node.Expr),
					pretty.Concat(
						prettyKeywordWithText("", "AS", " "),
						typ,
					),
				),
				")",
			),
		)

		if isCollatedString {
			ret = pretty.Fold(pretty.ConcatSpace,
				ret,
				pretty.Keyword("COLLATE"),
				pretty.Text(t.Locale))
		}
		return ret
	}
}

func (n *ValuesClause) doc(p *PrettyCfg) pretty.Doc {
	return p.rlTable(n.docTable(p)...)
}

func (n *ValuesClause) docTable(p *PrettyCfg) []pretty.RLTableRow {
	d := make([]pretty.Doc, len(n.Rows))
	for i := range n.Rows {
		d[i] = pretty.Bracket("(", p.Doc(&n.Rows[i]), ")")
	}
	return []pretty.RLTableRow{p.row("VALUES", pretty.Join(",", d...))}
}

func (node *StatementSource) doc(p *PrettyCfg) pretty.Doc {
	return pretty.Bracket("[", p.Doc(node.Statement), "]")
}

func (node *RowsFromExpr) doc(p *PrettyCfg) pretty.Doc {
	if p.Simplify && len(node.Items) == 1 {
		return p.Doc(node.Items[0])
	}
	return prettyBracketKeyword("ROWS FROM", " (", p.Doc(&node.Items), ")", "")
}

func (node *Array) doc(p *PrettyCfg) pretty.Doc {
	return prettyBracketKeyword("ARRAY", "[", p.Doc(&node.Exprs), "]", "")
}

func (node *Tuple) doc(p *PrettyCfg) pretty.Doc {
	exprDoc := p.Doc(&node.Exprs)
	if len(node.Exprs) == 1 {
		exprDoc = pretty.Concat(exprDoc, pretty.Text(","))
	}
	d := pretty.Bracket("(", exprDoc, ")")
	if len(node.Labels) > 0 {
		labels := make([]pretty.Doc, len(node.Labels))
		for i, n := range node.Labels {
			labels[i] = p.Doc((*Name)(&n))
		}
		d = pretty.Bracket("(", pretty.Stack(
			d,
			p.nestUnder(pretty.Keyword("AS"), pretty.Join(",", labels...)),
		), ")")
	}
	return d
}

func (node *UpdateExprs) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(n)
	}
	return pretty.Join(",", d...)
}

func (p *PrettyCfg) exprDocWithParen(e Expr) pretty.Doc {
	if _, ok := e.(operatorExpr); ok {
		return pretty.Bracket("(", p.Doc(e), ")")
	}
	return p.Doc(e)
}

func (n *Update) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 8)
	items = append(items,
		n.With.docRow(p),
		p.row("UPDATE", p.Doc(n.Table)),
		p.row("SET", p.Doc(&n.Exprs)))
	if len(n.From) > 0 {
		items = append(items,
			p.row("FROM", p.Doc(&n.From)))
	}
	items = append(items,
		n.Where.docRow(p),
		n.OrderBy.docRow(p))
	items = append(items, n.Limit.docTable(p)...)
	items = append(items, p.docReturning(n.Returning))
	return p.rlTable(items...)
}

func (n *Delete) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 6)
	items = append(items,
		n.With.docRow(p),
		p.row("DELETE FROM", p.Doc(n.Table)),
		n.Where.docRow(p),
		n.OrderBy.docRow(p))
	items = append(items, n.Limit.docTable(p)...)
	items = append(items, p.docReturning(n.Returning))
	return p.rlTable(items...)
}

func (p *PrettyCfg) docReturning(node ReturningClause) pretty.RLTableRow {
	switch r := node.(type) {
	case *NoReturningClause:
		return p.row("", nil)
	case *ReturningNothing:
		return p.row("RETURNING", pretty.Keyword("NOTHING"))
	case *ReturningExprs:
		return p.row("RETURNING", p.Doc((*SelectExprs)(r)))
	default:
		panic(fmt.Sprintf("unhandled case: %T", node))
	}
}

func (node *Order) doc(p *PrettyCfg) pretty.Doc {
	var d pretty.Doc
	if node.OrderType == OrderByColumn {
		d = p.Doc(node.Expr)
	} else {
		if node.Index == "" {
			d = pretty.ConcatSpace(
				pretty.Keyword("PRIMARY KEY"),
				p.Doc(&node.Table),
			)
		} else {
			d = pretty.ConcatSpace(
				pretty.Keyword("INDEX"),
				pretty.Fold(pretty.Concat,
					p.Doc(&node.Table),
					pretty.Text("@"),
					p.Doc(&node.Index),
				),
			)
		}
	}
	if node.Direction != DefaultDirection {
		d = p.nestUnder(d, pretty.Text(node.Direction.String()))
	}
	return d
}

func (node *UpdateExpr) doc(p *PrettyCfg) pretty.Doc {
	d := p.Doc(&node.Names)
	if node.Tuple {
		d = pretty.Bracket("(", d, ")")
	}
	e := node.Expr
	if p.Simplify {
		e = StripParens(e)
	}
	return p.nestUnder(d, pretty.ConcatSpace(pretty.Text("="), p.Doc(e)))
}

func (c *CreateTable) doc(p *PrettyCfg) pretty.Doc {
	title := "CREATE "
	if c.Temporary {
		title += "TEMPORARY "
	}
	title += "TABLE "
	if c.IfNotExists {
		title += "IF NOT EXISTS "
	}
	d := pretty.Concat(
		pretty.Keyword(title),
		p.Doc(&c.Table),
	)
	if c.As() {
		if len(c.AsColumnNames) > 0 {
			d = pretty.ConcatSpace(
				d,
				pretty.Bracket("(", p.Doc(&c.AsColumnNames), ")"),
			)
		}
		d = p.nestUnder(
			pretty.ConcatSpace(
				d,
				pretty.Keyword("AS"),
			),
			p.Doc(c.AsSource),
		)
	} else {
		if c.IsLike {
			docs := []pretty.Doc{pretty.ConcatSpace(
				d,
				pretty.Bracket("", p.Doc(&c.Defs), ""),
			)}
			d = pretty.Group(pretty.Stack(docs...))
		} else {
			docs := []pretty.Doc{pretty.ConcatSpace(
				d,
				pretty.Bracket("(", p.Doc(&c.Defs), ")"),
			)}
			if c.Interleave != nil {
				docs = append(docs, p.Doc(c.Interleave))
			}
			if c.PartitionBy != nil {
				docs = append(docs, p.Doc(c.PartitionBy))
			}
			d = pretty.Group(pretty.Stack(docs...))
		}
	}
	return d
}

func (n *CreateView) doc(p *PrettyCfg) pretty.Doc {
	title := "CREATE "
	if n.Temporary {
		title += "TEMPORARY "
	}
	if n.Materialized {
		title += "MATERIALIZED "
	}
	title += "VIEW"
	if n.IfNotExists {
		title += " IF NOT EXISTS"
	}
	d := pretty.ConcatSpace(
		pretty.Keyword(title),
		p.Doc(&n.Name),
	)
	if len(n.ColumnNames) > 0 {
		d = pretty.ConcatSpace(
			d,
			pretty.Bracket("(", p.Doc(&n.ColumnNames), ")"),
		)
	}
	return p.nestUnder(
		d,
		p.nestUnder(
			pretty.Keyword("AS"),
			p.Doc(n.AsSource),
		),
	)
}

func (node *TableDefs) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, len(*node))
	for i, n := range *node {
		d[i] = p.Doc(n)
	}
	return pretty.Join(",", d...)
}

func (node *CaseExpr) doc(p *PrettyCfg) pretty.Doc {
	d := make([]pretty.Doc, 0, len(node.Whens)+3)
	c := pretty.Keyword("CASE")
	if node.Expr != nil {
		c = pretty.Group(pretty.ConcatSpace(c, p.Doc(node.Expr)))
	}
	d = append(d, c)
	for _, when := range node.Whens {
		d = append(d, p.Doc(when))
	}
	if node.Else != nil {
		d = append(d, pretty.Group(pretty.ConcatSpace(
			pretty.Keyword("ELSE"),
			p.Doc(node.Else),
		)))
	}
	d = append(d, pretty.Keyword("END"))
	return pretty.Stack(d...)
}

func (node *When) doc(p *PrettyCfg) pretty.Doc {
	return pretty.Group(pretty.ConcatLine(
		pretty.Group(pretty.ConcatSpace(
			pretty.Keyword("WHEN"),
			p.Doc(node.Cond),
		)),
		pretty.Group(pretty.ConcatSpace(
			pretty.Keyword("THEN"),
			p.Doc(node.Val),
		)),
	))
}

func (n *UnionClause) doc(p *PrettyCfg) pretty.Doc {
	op := n.Type.String()
	if n.All {
		op += " ALL"
	}
	return pretty.Stack(p.Doc(n.Left), p.nestUnder(pretty.Keyword(op), p.Doc(n.Right)))
}

func (node *IfErrExpr) doc(p *PrettyCfg) pretty.Doc {
	var s string
	if node.Else != nil {
		s = "IFERROR"
	} else {
		s = "ISERROR"
	}
	d := []pretty.Doc{p.Doc(node.Cond)}
	if node.Else != nil {
		d = append(d, p.Doc(node.Else))
	}
	if node.ErrCode != nil {
		d = append(d, p.Doc(node.ErrCode))
	}
	return prettyBracketKeyword(s, "(", pretty.Join(",", d...), ")", "")
}

func (node *IfExpr) doc(p *PrettyCfg) pretty.Doc {
	return prettyBracketKeyword("IF", "(", pretty.Join(",",
		p.Doc(node.Cond),
		p.Doc(node.True),
		p.Doc(node.Else),
	), ")", "")
}

func (node *NullIfExpr) doc(p *PrettyCfg) pretty.Doc {
	return prettyBracketKeyword("NULLIF", "(", pretty.Join(",",
		p.Doc(node.Expr1),
		p.Doc(node.Expr2),
	), ")", "")
}

func (node *PartitionBy) doc(p *PrettyCfg) pretty.Doc {
	if node == nil {
		return pretty.Keyword("PARTITION BY NOTHING")
	}
	var title string
	if len(node.List) > 0 {
		if node.IsHash {
			title = `PARTITION BY HASH`
		} else {
			title = `PARTITION BY LIST`
		}
	} else if len(node.Range) > 0 {
		title = `PARTITION BY RANGE`
	}
	inner := make([]pretty.Doc, 0, len(node.List)+len(node.Range))
	for _, v := range node.List {
		inner = append(inner, p.Doc(&v))
	}
	for _, v := range node.Range {
		inner = append(inner, p.Doc(&v))
	}
	return pretty.ConcatSpace(
		prettyBracketKeyword(title, " (", p.Doc(&node.Fields), ")", ""),
		pretty.Bracket("(",
			pretty.Join(",", inner...),
			")",
		),
	)
}

func (node *ListPartition) doc(p *PrettyCfg) pretty.Doc {
	if node.IsHash {
		d := pretty.Fold(pretty.ConcatSpace,
			pretty.Keyword("PARTITION"),
			p.Doc(&node.Name),
		)
		if node.LocateSpaceName != nil {
			d = pretty.Fold(pretty.ConcatSpace,
				d,
				p.Doc(node.LocateSpaceName),
			)
		}
		if node.Subpartition != nil {
			d = p.nestUnder(d, p.Doc(node.Subpartition))
		}
		return d

	}
	d := pretty.Fold(pretty.ConcatSpace,
		pretty.Keyword("PARTITION"),
		p.Doc(&node.Name),
		prettyKeywordWithText("", "VALUES IN", " ("),
	)
	d = pretty.BracketDoc(
		d,
		p.Doc(&node.Exprs),
		pretty.Text(")"),
	)
	if node.Subpartition != nil {
		d = p.nestUnder(d, p.Doc(node.Subpartition))
	}
	if node.LocateSpaceName != nil {
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			p.Doc(node.LocateSpaceName),
		)
	}
	return d
}

func (node *RangePartition) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Fold(pretty.ConcatSpace,
		pretty.Keyword("PARTITION"),
		p.Doc(&node.Name),
		pretty.Keyword("VALUES"),
	)
	from := prettyBracketKeyword(
		"FROM", " (",
		p.Doc(&node.From),
		")", "",
	)
	to := prettyBracketKeyword(
		"TO", " (",
		p.Doc(&node.To),
		")", "",
	)
	d = p.nestUnder(d, pretty.Group(pretty.Stack(from, to)))
	if node.LocateSpaceName != nil {
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			p.Doc(node.LocateSpaceName),
		)
	}
	if node.Subpartition != nil {
		d = p.nestUnder(d, p.Doc(node.Subpartition))
	}
	return d
}

func (node *InterleaveDef) doc(p *PrettyCfg) pretty.Doc {
	title := pretty.Fold(
		pretty.ConcatSpace,
		pretty.Keyword("INTERLEAVE IN PARENT"),
		p.Doc(&node.Parent),
		pretty.Text("("),
	)
	d := pretty.BracketDoc(title, p.Doc(&node.Fields), pretty.Text(")"))
	if node.DropBehavior != DropDefault {
		d = pretty.ConcatSpace(d, pretty.Text(node.DropBehavior.String()))
	}
	return d
}

func (n *CreateIndex) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Keyword("CREATE")
	if n.Unique {
		d = pretty.ConcatSpace(d, pretty.Keyword("UNIQUE"))
	}
	if n.Inverted {
		d = pretty.ConcatSpace(d, pretty.Keyword("INVERTED"))
	}
	d = pretty.ConcatSpace(d, pretty.Keyword("INDEX"))
	if n.IfNotExists {
		d = pretty.ConcatSpace(d, pretty.Keyword("IF NOT EXISTS"))
	}
	if n.Name != "" {
		d = pretty.ConcatSpace(d, p.Doc(&n.Name))
	}
	docs := []pretty.Doc{
		pretty.Fold(
			pretty.ConcatSpace,
			d,
			pretty.Keyword("ON"),
			p.Doc(&n.Table),
			pretty.Bracket("(", p.Doc(&n.Columns), ")")),
	}

	if len(n.Storing) > 0 {
		docs = append(docs, prettyBracketKeyword(
			"STORING", " (",
			p.Doc(&n.Storing),
			")", "",
		))
	}
	if n.Interleave != nil {
		docs = append(docs, p.Doc(n.Interleave))
	}
	if n.IsLocal {
		docs = append(docs, pretty.Keyword(" LOCAL "))
	}
	if n.PartitionBy != nil {
		docs = append(docs, p.Doc(n.PartitionBy))
	}
	if n.LocateSpaceName != nil {
		docs = append(docs, p.Doc(n.LocateSpaceName))
	}
	return pretty.Group(pretty.Stack(docs...))
}

func (node *ColumnTableDef) doc(p *PrettyCfg) pretty.Doc {
	// TODO(knz): add a LLTable prettifier so types are aligned under each other.
	docs := make([]pretty.Doc, 0, 12)
	docs = append(docs, pretty.Text(coltypes.ColTypeAsString(node.Type)))
	if node.Nullable.Nullability != SilentNull && node.Nullable.ConstraintName != "" {
		docs = append(docs, pretty.ConcatSpace(
			pretty.Keyword("CONSTRAINT"),
			p.Doc(&node.Nullable.ConstraintName),
		))
	}
	switch node.Nullable.Nullability {
	case Null:
		docs = append(docs, pretty.Keyword("NULL"))
	case NotNull:
		docs = append(docs, pretty.Keyword("NOT NULL"))
	}
	if node.PrimaryKey || node.Unique {
		if node.UniqueConstraintName != "" {
			docs = append(docs, pretty.ConcatSpace(
				pretty.Keyword("CONSTRAINT"),
				p.Doc(&node.UniqueConstraintName),
			))
		}
		if node.PrimaryKey {
			docs = append(docs, pretty.Keyword("PRIMARY KEY"))
		} else if node.Unique {
			docs = append(docs, pretty.Keyword("UNIQUE"))
		}
	}
	if node.HasDefaultExpr() {
		if node.DefaultExpr.ConstraintName != "" {
			docs = append(docs, pretty.ConcatSpace(
				pretty.Keyword("CONSTRAINT"),
				p.Doc(&node.DefaultExpr.ConstraintName),
			))
		}
		docs = append(docs, pretty.ConcatSpace(
			pretty.Keyword("DEFAULT"),
			p.Doc(node.DefaultExpr.Expr),
		))
	}
	for _, checkExpr := range node.CheckExprs {
		d := prettyBracketKeyword(
			"CHECK", " (",
			p.Doc(checkExpr.Expr),
			")", "",
		)
		if checkExpr.ConstraintName != "" {
			d = p.nestUnder(
				pretty.ConcatSpace(
					pretty.Keyword("CONSTRAINT"),
					p.Doc(&checkExpr.ConstraintName),
				),
				d,
			)
		}
		docs = append(docs, d)
	}
	if node.HasFKConstraint() {
		d := pretty.Nil
		if node.References.ConstraintName != "" {
			d = pretty.Fold(pretty.ConcatSpace,
				d,
				pretty.Keyword("CONSTRAINT"),
				p.Doc(&node.References.ConstraintName),
			)
		}
		d = pretty.Fold(pretty.ConcatSpace,
			d,
			pretty.Keyword("REFERENCES"),
			p.Doc(node.References.Table),
		)
		if node.References.Col != "" {
			d = pretty.ConcatSpace(
				d,
				pretty.Bracket(
					"(",
					p.Doc(&node.References.Col),
					")",
				),
			)
		}
		// We omit MATCH SIMPLE because it is the default.
		if node.References.Match != MatchSimple {
			d = pretty.ConcatSpace(d, pretty.Text(node.References.Match.String()))
		}
		if ref := p.Doc(&node.References.Actions); ref != pretty.Nil {
			d = p.nestUnder(d, ref)
		}
		docs = append(docs, d)
	}
	if node.IsComputed() {
		docs = append(docs, prettyBracketKeyword(
			"AS", " (",
			p.Doc(node.Computed.Expr),
			") ", "STORED",
		))
	}
	if node.HasColumnFamily() {
		d := pretty.Nil
		if node.Family.Create {
			d = pretty.ConcatSpace(d, pretty.Keyword("CREATE"))
		}
		if node.Family.IfNotExists {
			d = pretty.ConcatSpace(d, pretty.Keyword("IF NOT EXISTS"))
		}
		d = pretty.ConcatSpace(d, pretty.Keyword("FAMILY"))
		if len(node.Family.Name) > 0 {
			d = pretty.ConcatSpace(d, p.Doc(&node.Family.Name))
		}
		docs = append(docs, d)
	}
	return p.nestUnder(
		p.Doc(&node.Name),
		pretty.Stack(docs...),
	)
}

func (node *CheckConstraintTableDef) doc(p *PrettyCfg) pretty.Doc {
	d := prettyBracketKeyword(
		"CHECK", " (",
		p.Doc(node.Expr),
		")", "",
	)
	if node.Name != "" {
		d = p.nestUnder(
			pretty.ConcatSpace(
				pretty.Keyword("CONSTRAINT"),
				p.Doc(&node.Name),
			),
			d,
		)
	}
	return d
}

func (node *ReferenceActions) doc(p *PrettyCfg) pretty.Doc {
	var docs []pretty.Doc
	if node.Delete != NoAction {
		docs = append(docs, pretty.ConcatSpace(
			pretty.Keyword("ON DELETE"),
			pretty.Text(node.Delete.String()),
		))
	}
	if node.Update != NoAction {
		docs = append(docs, pretty.ConcatSpace(
			pretty.Keyword("ON UPDATE"),
			pretty.Text(node.Update.String()),
		))
	}
	return pretty.Fold(pretty.ConcatSpace, docs...)
}

func (n *Backup) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 0, 6)

	items = append(items, p.row("BACKUP", pretty.Nil))
	items = append(items, n.Targets.docRow(p))
	items = append(items, p.row("TO", p.Doc(n.To)))

	if n.AsOf.Expr != nil {
		items = append(items, n.AsOf.docRow(p))
	}
	if n.IncrementalFrom != nil {
		items = append(items, p.row("INCREMENTAL FROM", p.Doc(&n.IncrementalFrom)))
	}
	if n.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&n.Options)))
	}
	return p.rlTable(items...)
}

func (node *TargetList) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *TargetList) docRow(p *PrettyCfg) pretty.RLTableRow {
	if node.Databases != nil {
		return p.row("DATABASE", p.Doc(&node.Databases))
	}
	return p.row("TABLE", p.Doc(&node.Tables))
}

func (node *AsOfClause) doc(p *PrettyCfg) pretty.Doc {
	return p.unrow(node.docRow(p))
}

func (node *AsOfClause) docRow(p *PrettyCfg) pretty.RLTableRow {
	return p.row("AS OF SYSTEM TIME", p.Doc(node.Expr))
}

func (o *KVOptions) doc(p *PrettyCfg) pretty.Doc {
	var opts []pretty.Doc
	for _, opt := range *o {
		d := p.Doc(&opt.Key)
		if opt.Value != nil {
			d = pretty.Fold(pretty.ConcatSpace,
				d,
				pretty.Text("="),
				p.Doc(opt.Value),
			)
		}
		opts = append(opts, d)
	}
	return pretty.Join(",", opts...)
}

func (n *Export) doc(p *PrettyCfg) pretty.Doc {
	items := make([]pretty.RLTableRow, 0, 5)
	items = append(items, p.row("EXPORT", pretty.Nil))
	items = append(items, p.row("INTO "+n.FileFormat, p.Doc(n.File)))
	if n.Options != nil {
		items = append(items, p.row("WITH", p.Doc(&n.Options)))
	}
	items = append(items, p.row("FROM", p.Doc(n.Query)))
	return p.rlTable(items...)
}

func (n *Explain) doc(p *PrettyCfg) pretty.Doc {
	d := pretty.Keyword("EXPLAIN")
	if len(n.Options) > 0 {
		var opts []pretty.Doc
		for _, opt := range n.Options {
			upperCaseOpt := strings.ToUpper(opt)
			if upperCaseOpt == "ANALYZE" {
				d = pretty.ConcatSpace(d, pretty.Keyword("ANALYZE"))
			} else {
				opts = append(opts, pretty.Keyword(upperCaseOpt))
			}
		}
		d = pretty.ConcatSpace(
			d,
			pretty.Bracket("(", pretty.Join(",", opts...), ")"),
		)
	}
	return p.nestUnder(d, p.Doc(n.Statement))
}

func (node *NotExpr) doc(p *PrettyCfg) pretty.Doc {
	return p.nestUnder(
		pretty.Keyword("NOT"),
		p.exprDocWithParen(node.Expr),
	)
}

func (node *CoalesceExpr) doc(p *PrettyCfg) pretty.Doc {
	return prettyBracketKeyword(
		node.Name, "(",
		p.Doc(&node.Exprs),
		")", "",
	)
}

func (node *CharExpr) doc(p *PrettyCfg) pretty.Doc {
	return prettyBracketKeyword(
		node.Name, "(",
		p.Doc(&node.Exprs),
		")", "",
	)
}

func (node *NvlExpr) doc(p *PrettyCfg) pretty.Doc {
	return prettyBracketKeyword(
		node.Name, "(",
		p.Doc(&node.Exprs),
		")", "",
	)
}
