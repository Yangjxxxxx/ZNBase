// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015  The Cockroach Authors.
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

// This code was derived from https://github.com/youtube/vitess.

package parser

import (
	"fmt"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/coltypes"
	"github.com/znbasedb/znbase/pkg/sql/pgwire/pgerror"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
)

// Statement is the result of parsing a single statement. It contains the AST
// node along with other information.
type Statement struct {
	// AST is the root of the AST tree for the parsed statement.
	AST tree.Statement

	// SQL is the original SQL from which the statement was parsed. Note that this
	// is not appropriate for use in logging, as it may contain passwords and
	// other sensitive data.
	SQL string

	// NumPlaceholders indicates the number of arguments to the statement (which
	// are referenced through placeholders). This corresponds to the highest
	// argument position (i.e. the x in "$x") that appears in the query.
	//
	// Note: where there are "gaps" in the placeholder positions, this number is
	// based on the highest position encountered. For example, for `SELECT $3`,
	// NumPlaceholders is 3. These cases are malformed and will result in a
	// type-check error.
	NumPlaceholders int

	// IsPortal: Whether Type of The Executor is ExecPortal
	IsExecPortal bool
}

// Statements is a list of parsed statements.
type Statements []Statement

// String returns the AST formatted as a string.
func (stmts Statements) String() string {
	return stmts.StringWithFlags(tree.FmtSimple)
}

// StringWithFlags returns the AST formatted as a string (with the given flags).
func (stmts Statements) StringWithFlags(flags tree.FmtFlags) string {
	ctx := tree.NewFmtCtx(flags)
	for i, s := range stmts {
		if i > 0 {
			ctx.WriteString("; ")
		}
		ctx.FormatNode(s.AST)
	}
	return ctx.CloseAndGetString()
}

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    scanner
	lexer      lexer
	parserImpl sqlParserImpl
	tokBuf     [8]sqlSymType
	stmtBuf    [1]Statement
	selExprs   tree.SelectExprs
}

//SetCasesensitive 改变parser中大小写敏感开关状态
func (p *Parser) SetCasesensitive(b bool) {
	p.scanner.CaseSensitive = b
}

// INT8 is the historical interpretation of INT. This should be left
// alone in the future, since there are many sql fragments stored
// in various descriptors.  Any user input that was created after
// INT := INT4 will simply use INT4 in any resulting code.
var defaultNakedIntType = coltypes.Int8
var defaultNakedSerialType = coltypes.Serial8

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (Statements, error) {
	return p.parseWithDepth(1, sql, defaultNakedIntType, defaultNakedSerialType)
}

// ParseWithInt parses a sql statement string and returns a list of
// Statements. The INT token will result in the specified TInt type.
func (p *Parser) ParseWithInt(sql string, nakedIntType *coltypes.TInt) (Statements, error) {
	p.scanner.isClientQuery = true
	nakedSerialType := coltypes.Serial8
	if nakedIntType == coltypes.Int4 {
		nakedSerialType = coltypes.Serial4
	}
	return p.parseWithDepth(1, sql, nakedIntType, nakedSerialType)
}

func (p *Parser) parseOneWithDepth(depth int, sql string) (Statement, error) {
	stmts, err := p.parseWithDepth(1, sql, defaultNakedIntType, defaultNakedSerialType)
	if err != nil {
		return Statement{}, err
	}
	if len(stmts) != 1 {
		return Statement{}, pgerror.NewAssertionErrorf("expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

func (p *Parser) scanOneStmt() (sql string, tokens []sqlSymType, done bool) {
	var lval sqlSymType
	tokens = p.tokBuf[:0]

	// Scan the first token.
	for {
		p.scanner.scan(&lval)
		tampStr := strings.ToLower(lval.str)
		if tampStr == "select" {
			p.scanner.isFirstSelect = true
		} else {
			p.scanner.isFirstSelect = false
		}
		if lval.id == 0 {
			return "", nil, true
		}
		if lval.id != ';' {
			break
		}
	}

	startPos := lval.pos
	// We make the resulting token positions match the returned string.
	lval.pos = 0
	tokens = append(tokens, lval)
	for {
		if lval.id == ERROR {
			return p.scanner.in[startPos:], tokens, true
		}
		posBeforeScan := p.scanner.pos
		lval.union.isUp = false
		p.scanner.scan(&lval)
		if lval.id == 0 || lval.id == ';' {
			p.scanner.isToChange = false
			return p.scanner.in[startPos:posBeforeScan], tokens, (lval.id == 0)
		}
		lval.pos -= startPos

		if lval.str == "from" && p.scanner.isClientQuery && !tree.CaseSensitiveG && !p.scanner.CaseSensitive {
			if tokens[0].str == "select" {
				tampStrList := []string{}
				if find := strings.Contains(p.scanner.in, "'"); find {
					str := strings.Split(p.scanner.in, "'")
					tampStrList = append(tampStrList, str[1:len(str)-1]...)
				}
				for i := len(tokens) - 1; i >= 0; i-- {
					if tokens[i].str == "select" {
						tampStr := tokens[i+1:]
						for key, value := range tampStr {
							if value.union.isUp {
								done := false
								n := key
								for n != 0 && tampStr[n-1].str != "," && tampStr[n-1].str != "(" && tampStr[n-1].str != "as" {
									n = n - 1
									done = true
								}
								if done {
									tampStr[n].union.isUp = true
									if n+2 <= len(tampStr)-1 {
										tampStr[n+2].union.isUp = true
									}
								}
								if key >= 2 && tampStr[key-1].str == "as" {
									tampStr[key-2].union.AsNameIsUp = true
									if tampStr[key-2].str == ")" {
										m := key - 2
										done = false
										for m != 0 && tampStr[m-1].str != "(" {
											m = m - 1
											done = true
										}
										if done {
											if m >= 2 {
												tampStr[m-2].union.AsNameIsUp = true
											}
										}
									}
								}
							}
							switch value.str {
							case "+", "-", "/", "|", "<", ">", "=", "<=", ">=":
								if key != 0 && !tampStr[key-1].union.isUp && !p.checkIsExistSame(tampStrList, tampStr, value, key, "-") && tampStr[key-1].id != 57348 {
									tampStr[key-1].str = strings.ToLower(tampStr[key-1].str)
								}
								if !tampStr[key+1].union.isUp && !p.checkIsExistSame(tampStrList, tampStr, value, key, "+") && tampStr[key+1].id != 57348 {
									tampStr[key+1].str = strings.ToLower(tampStr[key+1].str)
								}
							case "*":
								if key != 0 && key != len(tampStr)-1 && tampStr[key+1].str != "," {
									if !tampStr[key-1].union.isUp && !p.checkIsExistSame(tampStrList, tampStr, value, key, "-") && tampStr[key-1].id != 57348 {
										tampStr[key-1].str = strings.ToLower(tampStr[key-1].str)
									}
									if !tampStr[key+1].union.isUp && !p.checkIsExistSame(tampStrList, tampStr, value, key, "+") && tampStr[key+1].id != 57348 {
										tampStr[key+1].str = strings.ToLower(tampStr[key+1].str)
									}
								}
							case ".":
								if key != 0 && !tampStr[key-1].union.isUp && !p.checkIsExistSame(tampStrList, tampStr, value, key, "-") {
									tampStr[key-1].str = strings.ToLower(tampStr[key-1].str)
								}
							case "(":
								p.scanner.isParentheses = true
								if key != 0 && !tampStr[key-1].union.isUp {
									tampStr[key-1].str = strings.ToLower(tampStr[key-1].str)
								}
							case ")":
								p.scanner.isParentheses = false
							}
							if p.scanner.isParentheses {
								if !value.union.isUp && !p.checkIsExistSame(tampStrList, tampStr, value, key, "") {
									tampStr[key].str = strings.ToLower(tampStr[key].str)
								}
							}
						}
						break
					}
				}
			}
		} else if lval.str == "(" {
			if tokens[0].str == "select" {
				tampStrList := []string{}
				if find := strings.Contains(p.scanner.in, "\""); find {
					str := strings.Split(p.scanner.in, "\"")
					tampStrList = append(tampStrList, str[1:len(str)-1]...)
				}
				isSame := false
				if len(tampStrList) != 0 {
					for _, val := range tampStrList {
						if val == tokens[len(tokens)-1].str {
							isSame = true
						}
					}
				}
				if !isSame {
					tokens[len(tokens)-1].str = strings.ToLower(tokens[len(tokens)-1].str)
				}
			}
		}

		tokens = append(tokens, lval)
	}
}

func (p *Parser) checkIsExistSame(
	tampStrList []string, tampStr []sqlSymType, value sqlSymType, key int, mode string,
) bool {
	isSame := false
	if len(tampStrList) == 0 {
		return false
	}

	switch mode {
	case "+":
		for _, val := range tampStrList {
			if val == tampStr[key+1].str {
				isSame = true
			}
		}
		return isSame
	case "-":
		for _, val := range tampStrList {
			if val == tampStr[key-1].str {
				isSame = true
			}
		}
		return isSame
	default:
		for _, val := range tampStrList {
			if val == value.str {
				isSame = true
			}
		}
		return isSame
	}
}

func (p *Parser) parseWithDepth(
	depth int, sql string, nakedIntType *coltypes.TInt, nakedSerialType *coltypes.TSerial,
) (Statements, error) {
	stmts := Statements(p.stmtBuf[:0])
	p.scanner.init(sql)
	defer p.scanner.cleanup()
	for {
		sql, tokens, done := p.scanOneStmt()
		stmt, err := p.parse(depth+1, sql, tokens, nakedIntType, nakedSerialType)
		if err != nil {
			return nil, err
		}

		if stmt.AST != nil && p.scanner.isClientQuery && !tree.CaseSensitiveG && !p.scanner.CaseSensitive {
			switch s := stmt.AST.(type) {
			case *tree.Select:
				p.reconstructSelect(s, stmt.SQL, false)
			}
			p.selExprs = nil
			p.scanner.asNameList = nil
		}

		if stmt.AST != nil {
			stmts = append(stmts, stmt)
		}
		if done {
			break
		}
	}
	return stmts, nil
}

func (p *Parser) reconstructSelect(s *tree.Select, sql string, isSub bool) {
	switch sel := s.Select.(type) {
	case *tree.SelectClause:
		if sel.From != nil && len(sel.From.Tables) != 0 {
			p.reconstructFromExpr(sel.From.Tables, sql)
		}
		p.reconstructWhereExpr(sel.Where, sql)
		p.reconstructSelExpr(sel, isSub)
		p.selExprs = sel.Exprs
	case *tree.UnionClause:
		p.reconstructSelect(sel.Left, sql, false)
		p.reconstructSelect(sel.Right, sql, false)
	}
}

func (p *Parser) reconstructFromExpr(tables tree.TableExprs, sql string) {
	p.reconstructSubquryExpr(tables[0], sql)
	tables = tables[1:]
	if len(tables) == 0 {
		return
	}
	p.reconstructFromExpr(tables, sql)
}

func (p *Parser) reconstructSubquryExpr(texpr tree.TableExpr, sql string) {
	switch source := texpr.(type) {
	case *tree.AliasedTableExpr:
		p.reconstructSubquryExpr(source.Expr, sql)
	case *tree.Subquery:
		switch parent := source.Select.(type) {
		case *tree.ParenSelect:
			p.reconstructSelect(parent.Select, sql, true)
		}
	}
}

func (p *Parser) reconstructWhereExpr(where *tree.Where, sql string) {
	if where != nil && where.Expr != nil {
		switch t := where.Expr.(type) {
		case *tree.ComparisonExpr:
			p.resolveComparisonExpr(t, sql)
		case *tree.AndExpr:
			p.resolveAndExpr(t, sql)
		}
	}
}

func (p *Parser) resolveComparisonExpr(expr *tree.ComparisonExpr, sql string) {
	switch l := expr.Left.(type) {
	case *tree.Subquery:
		switch parent := l.Select.(type) {
		case *tree.ParenSelect:
			p.reconstructSelect(parent.Select, sql, true)
		}
	case *tree.ComparisonExpr:
		p.resolveComparisonExpr(l, sql)
	}

	switch r := expr.Right.(type) {
	case *tree.Subquery:
		switch parent := r.Select.(type) {
		case *tree.ParenSelect:
			p.reconstructSelect(parent.Select, sql, true)
		}
	case *tree.ComparisonExpr:
		p.resolveComparisonExpr(r, sql)
	}
}

func (p *Parser) resolveAndExpr(expr *tree.AndExpr, sql string) {
	switch l := expr.Left.(type) {
	case *tree.ComparisonExpr:
		p.resolveComparisonExpr(l, sql)
	case *tree.AndExpr:
		p.resolveAndExpr(l, sql)
	}

	switch r := expr.Right.(type) {
	case *tree.ComparisonExpr:
		p.resolveComparisonExpr(r, sql)
	case *tree.AndExpr:
		p.resolveAndExpr(r, sql)
	}
}

func (p *Parser) reconstructExpr(
	v *tree.UnresolvedName, sel *tree.SelectClause, val tree.SelectExpr, key int,
) {
	if val.As != "" {
		p.scanner.asNameList = append(p.scanner.asNameList, string(val.As))
		sel.AsNameList = p.scanner.asNameList
	}
	if v.Parts[0] != "" {
		//Column name without double quotes
		if !v.IsDoublequotes {
			sel.Exprs[key].As = upToLow(v.Parts[0], val, v)
		}
	}
}

func (p *Parser) reconstructSelExpr(sel *tree.SelectClause, isSub bool) {
	for key, val := range sel.Exprs {
		switch v := val.Expr.(type) {
		case *tree.UnresolvedName:
			p.reconstructExpr(v, sel, val, key)
		case *tree.CastExpr:
			switch t := v.Expr.(type) {
			case *tree.UnresolvedName:
				p.reconstructExpr(t, sel, val, key)
			}
		case *tree.AnnotateTypeExpr:
			switch t := v.Expr.(type) {
			case *tree.UnresolvedName:
				p.reconstructExpr(t, sel, val, key)
			}
		}
	}

	if p.selExprs != nil {
		for key := range sel.Exprs {
			switch t := sel.Exprs[key].Expr.(type) {
			case *tree.UnresolvedName:
				isSame := false
				outAsNameExist := checkAsNameIsExist(p.scanner.asNameList, string(sel.Exprs[key].As))
				for num, value := range p.selExprs {
					switch value.Expr.(type) {
					//example: select abc from (select bb as ABC from ...)
					//example: select abc from (select 'bb' as ABC from ...)
					default:
						if strings.ToLower(t.Parts[0]) == strings.ToLower(string(value.As)) {
							switch t.IsDoublequotes {
							case true:
								isUp := checkIsUp(t.Parts[0])
								switch outAsNameExist {
								case true:
									if !value.AsNameIsDoublequotes {
										p.selExprs[num].As = tree.UnrestrictedName(strings.ToLower(string(value.As)))
									}
								case false:
									if !value.AsNameIsDoublequotes {
										if isUp {
											p.selExprs[num].As = tree.UnrestrictedName(strings.ToLower(string(value.As)))
										} else {
											sel.Exprs[key].As = value.As
											p.selExprs[num].As = tree.UnrestrictedName(strings.ToLower(string(value.As)))
										}
									}
								}
							case false:
								switch outAsNameExist {
								case true:
									if !value.AsNameIsDoublequotes {
										p.selExprs[num].As = tree.UnrestrictedName(strings.ToLower(string(value.As)))
										t.Parts[0] = strings.ToLower(string(value.As))
									}
								case false:
									isUp := checkIsUp(string(value.As))
									if !value.AsNameIsDoublequotes {
										sel.Exprs[key].As = value.As
										p.selExprs[num].As = tree.UnrestrictedName(strings.ToLower(string(value.As)))
										t.Parts[0] = strings.ToLower(string(value.As))
									} else {
										if !isUp {
											sel.Exprs[key].As = value.As
											t.Parts[0] = string(value.As)
										}
									}
								}
							}
							isSame = true
						}
					}
				}
				if !isSame {
					for num, value := range p.selExprs {
						switch k := value.Expr.(type) {
						case *tree.UnresolvedName:
							//example: select "abc" from (select ABC from ...)
							if t.Parts[0] == k.Parts[0] {
								switch value.As {
								case "":
									//The outer alias is not a customer input
									if !outAsNameExist {
										sel.Exprs[key].As = tree.UnrestrictedName(k.Parts[0])
										if k.IsDoublequotes {
											sel.Exprs[key].AsNameIsDoublequotes = true
										}
									}
								}
							} else if t.IsDoublequotes && !value.AsNameIsDoublequotes && strings.ToLower(t.Parts[0]) == k.Parts[0] {
								//example: select "ABC" from (select abc as ABC from ...) report error
								p.selExprs[num].As = ""
							}
						}
					}
				}
			}
		}

	}
}

//Determine whether AsName is user input
func checkAsNameIsExist(asNameList []string, asName string) bool {
	for _, value := range asNameList {
		if asName == value {
			return true
		}
	}
	return false
}

func checkIsUp(str string) bool {
	for _, value := range str {
		if value >= 'A' && value <= 'Z' {
			return true
		}
	}
	return false
}

//Determine whether the column name is uppercase, convert the column name to lowercase,
//and assign the uppercase column name to AsName.
func upToLow(tampStr string, val tree.SelectExpr, expr *tree.UnresolvedName) tree.UnrestrictedName {
	isUp := checkIsUp(tampStr)
	if isUp {
		expr.OriginalName = expr.Parts[0]
		expr.Parts[0] = strings.ToLower(expr.Parts[0])
		if val.As == "" {
			return tree.UnrestrictedName(tampStr)
		}
	}
	return val.As
}

// parse parses a statement from the given scanned tokens.
func (p *Parser) parse(
	depth int,
	sql string,
	tokens []sqlSymType,
	nakedIntType *coltypes.TInt,
	nakedSerialType *coltypes.TSerial,
) (Statement, error) {
	p.lexer.init(sql, tokens, nakedIntType, nakedSerialType)
	defer p.lexer.cleanup()
	if p.parserImpl.Parse(&p.lexer) != 0 {
		if p.lexer.lastError == nil {
			// This should never happen -- there should be an error object
			// every time Parse() returns nonzero. We're just playing safe
			// here.
			p.lexer.Error("syntax error")
		}
		err := p.lexer.lastError
		if err.TelemetryKey != "" {
			// TODO(knz): move the auto-prefixing of feature names to a
			// higher level in the call stack.
			err.TelemetryKey = "syntax." + err.TelemetryKey
		}
		err.ResetSource(depth + 1)
		return Statement{}, err
	}
	return Statement{
		AST:             p.lexer.stmt,
		SQL:             sql,
		NumPlaceholders: p.lexer.numPlaceholders,
	}, nil
}

// unaryNegation constructs an AST node for a negation. This attempts
// to preserve constant NumVals and embed the negative sign inside
// them instead of wrapping in an UnaryExpr. This in turn ensures
// that negative numbers get considered as a single constant
// for the purpose of formatting and scrubbing.
func unaryNegation(e tree.Expr) tree.Expr {
	if cst, ok := e.(*tree.NumVal); ok {
		cst.Negative = !cst.Negative
		return cst
	}

	// Common case.
	return &tree.UnaryExpr{Operator: tree.UnaryMinus, Expr: e}
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string, cs bool) (Statements, error) {
	var p Parser
	p.SetCasesensitive(cs)
	return p.parseWithDepth(1, sql, defaultNakedIntType, defaultNakedSerialType)
}

// ParseOne parses a sql statement string, ensuring that it contains only a
// single statement, and returns that Statement. ParseOne will always
// interpret the INT and SERIAL types as 64-bit types, since this is
// used in various internal-execution paths where we might receive
// bits of SQL from other nodes. In general, we expect that all
// user-generated SQL has been run through the ParseWithInt() function.
func ParseOne(sql string, cs bool) (Statement, error) {
	var p Parser
	p.SetCasesensitive(cs)
	return p.parseOneWithDepth(1, sql)
}

// ParseTableIndexName parses a table name with index.
func ParseTableIndexName(sql string) (tree.TableIndexName, error) {
	// We wrap the name we want to parse into a dummy statement since our parser
	// can only parse full statements.
	stmt, err := ParseOne(fmt.Sprintf("ALTER INDEX %s RENAME TO x", sql), false)
	if err != nil {
		return tree.TableIndexName{}, err
	}
	rename, ok := stmt.AST.(*tree.RenameIndex)
	if !ok {
		return tree.TableIndexName{}, pgerror.NewAssertionErrorf("expected an ALTER INDEX statement, but found %T", stmt)
	}
	return *rename.Index, nil
}

// ParseTableName parses a table name.
func ParseTableName(sql string, isSequence int) (*tree.TableName, error) {
	// We wrap the name we want to parse into a dummy statement since our parser
	// can only parse full statements.
	if isSequence == 1 {
		sql = AddExtraMark(sql, '"')
		sql = AddExtraMark2(sql, '.')
	}
	stmt, err := ParseOne(fmt.Sprintf("ALTER TABLE %s RENAME TO x", sql), false)
	if err != nil {
		return nil, err
	}
	rename, ok := stmt.AST.(*tree.RenameTable)
	if !ok {
		return nil, pgerror.NewAssertionErrorf("expected an ALTER TABLE statement, but found %T", stmt)
	}
	return &rename.Name, nil
}

// AddExtraMark2 为字符串某个子字符串两边增加单/双引号
func AddExtraMark2(name string, signal byte) string {
	for i := 0; i < len(name); i++ {
		if name[i] == signal {
			tempright := name[i+1:]
			templeft := name[:i] + string('"') + string(name[i]) + string('"')
			name = templeft + tempright
			i += 2
		}
	}
	return name
}

// AddExtraMark 为字符串增加单/双引号
func AddExtraMark(name string, signal byte) string {
	for i := 0; i < len(name); i++ {
		if name[i] == signal {
			name = name[:i] + string('"') + name[i:]
			i++
		}
	}
	name = name + string('"')
	name = string('"') + name
	return name
}

// parseExprs parses one or more sql expressions.
func parseExprs(exprs []string) (tree.Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")), false)
	if err != nil {
		return nil, err
	}
	set, ok := stmt.AST.(*tree.SetVar)
	if !ok {
		return nil, pgerror.NewAssertionErrorf("expected a SET statement, but found %T", stmt)
	}
	return set.Values, nil
}

// ParseExprs is a short-hand for parseExprs(sql)
func ParseExprs(sql []string) (tree.Exprs, error) {
	if len(sql) == 0 {
		return tree.Exprs{}, nil
	}
	return parseExprs(sql)
}

// ParseExpr is a short-hand for parseExprs([]string{sql})
func ParseExpr(sql string) (tree.Expr, error) {
	exprs, err := parseExprs([]string{sql})
	if err != nil {
		return nil, err
	}
	if len(exprs) != 1 {
		return nil, pgerror.NewAssertionErrorf("expected 1 expression, found %d", len(exprs))
	}
	return exprs[0], nil
}

// ParseType parses a column type.
func ParseType(sql string) (coltypes.CastTargetType, error) {
	expr, err := ParseExpr(fmt.Sprintf("1::%s", sql))
	if err != nil {
		return nil, err
	}

	cast, ok := expr.(*tree.CastExpr)
	if !ok {
		return nil, pgerror.NewAssertionErrorf("expected a tree.CastExpr, but found %T", expr)
	}

	return cast.Type, nil
}
