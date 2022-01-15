package tree

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/znbasedb/znbase/pkg/sql/sem/types"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

// 需要啥属性就往里面添
var error1 = "error1"
var error2 = "error2"
var joinCondForUnion = &OnJoinCond{
	Expr: &ComparisonExpr{
		Operator:       EQ,
		SubOperator:    EQ,
		Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
		Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
		typeAnnotation: typeAnnotation{Typ: nil},
		fn:             nil,
	},
}

type insertTest struct {
	// testOrNot  switch for testing
	testOrNot bool
	// name represent the name of this case
	name string
	// 各种input
	oc OnConflict
	// input for ConstructCaseExprByColName
	inputForCCEC CCEC
	// input for ConstructCaseExprOfAdditionalCol
	inputForCCEA CCEA
	// input for GetUnionColsOfNotMatch
	inputForGUCNM GUCNM
	// expect for ConstructCaseExprByColName & ConstructCaseExprOfAdditionalCol
	expectForCaseExpr CaseExpr
	// expect for GetUnionColsOfNotMatch & GetUnionColsOfMatch
	expectError   bool
	expectForCols []Name
}

type CCEC struct {
	colName    Name
	colOrd     int
	tableName  string
	matchOrNot bool
}

type CCEA struct {
	tableName      string
	primaryKeyName string
}

type GUCNM struct {
	colNames      []Name
	hasPrimaryKey bool
}

var testCase = []insertTest{
	// match
	{
		testOrNot: true,
		name:      "ConstructCaseExprByColName",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: &OnJoinCond{
				Expr: &ComparisonExpr{
					Operator:       EQ,
					SubOperator:    EQ,
					Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
					Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
					typeAnnotation: typeAnnotation{Typ: nil},
					fn:             nil,
				},
			},
			MatchList: CondAndOpList{
				{
					Condition: nil,
					Operation: UpdateExprs{
						{
							Tuple: false,
							Names: NameList{
								"salary",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					IsDelete: false,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForCCEC: CCEC{
			colName:    "salary",
			colOrd:     2,
			tableName:  "em",
			matchOrNot: true,
		},
		expectForCaseExpr: CaseExpr{
			Expr: nil,
			Whens: []*When{
				{
					Cond: Datum(DBoolTrue),
					Val: &UnresolvedName{
						NumParts: 2,
						Star:     false,
						Parts:    NameParts{"salary", "ma", "", ""},
					},
				},
			},
			Else: &UnresolvedName{
				NumParts: 2,
				Star:     false,
				Parts:    NameParts{"salary", "em", "", ""},
			},
			typeAnnotation: typeAnnotation{Typ: nil},
		},
	},
	// not match
	{
		testOrNot: true,
		name:      "ConstructCaseExprByColName",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: &OnJoinCond{
				Expr: &ComparisonExpr{
					Operator:       EQ,
					SubOperator:    EQ,
					Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
					Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
					typeAnnotation: typeAnnotation{Typ: nil},
					fn:             nil,
				},
			},
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: nil,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForCCEC: CCEC{
			colName:    "employeid",
			colOrd:     0,
			tableName:  "em",
			matchOrNot: false,
		},
		expectForCaseExpr: CaseExpr{
			Expr: nil,
			Whens: []*When{
				{
					Cond: Datum(DBoolTrue),
					Val: &UnresolvedName{
						NumParts: 2,
						Star:     false,
						Parts:    NameParts{"managerid", "ma", "", ""},
					},
				},
			},
			Else: &UnresolvedName{
				NumParts: 2,
				Star:     false,
				Parts:    NameParts{"employeid", "em", "", ""},
			},
			typeAnnotation: typeAnnotation{Typ: nil},
		},
	},
	// match delete
	{
		testOrNot: true,
		name:      "ConstructCaseExprForAdditionalCol",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: &OnJoinCond{
				Expr: &ComparisonExpr{
					Operator:       EQ,
					SubOperator:    EQ,
					Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
					Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
					typeAnnotation: typeAnnotation{Typ: nil},
					fn:             nil,
				},
			},
			MatchList: CondAndOpList{
				{
					Condition: nil,
					Operation: nil,
					IsDelete:  true,
					Sig:       nil,
				},
			},
			MergeOrNot: true,
		},
		inputForCCEA: CCEA{
			tableName:      "em",
			primaryKeyName: "rowid",
		},
		expectForCaseExpr: CaseExpr{
			Expr: nil,
			Whens: []*When{
				{
					Cond: &ComparisonExpr{
						Operator:       EQ,
						SubOperator:    EQ,
						Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
						Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
						typeAnnotation: typeAnnotation{Typ: nil},
						fn:             nil,
					},
					Val: &AdditionalExpr{
						Expr:           nil,
						OperationType:  1,
						Sig:            nil,
						typeAnnotation: typeAnnotation{Typ: types.Int},
					},
				},
				{
					Cond: &AndExpr{
						Left: Datum(DBoolTrue),
						Right: &ComparisonExpr{
							Operator:       IsNotDistinctFrom,
							SubOperator:    EQ,
							Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"rowid", "em", "", ""}},
							Right:          DNull,
							typeAnnotation: typeAnnotation{Typ: nil},
							fn:             nil,
						},
						typeAnnotation: typeAnnotation{Typ: nil},
					},
					Val: &AdditionalExpr{
						Expr:           nil,
						OperationType:  3,
						Sig:            nil,
						typeAnnotation: typeAnnotation{Typ: types.Int},
					},
				},
			},
			Else: &AdditionalExpr{
				Expr:           nil,
				OperationType:  0,
				Sig:            nil,
				typeAnnotation: typeAnnotation{Typ: types.Int},
			},
			typeAnnotation: typeAnnotation{Typ: nil},
		},
	},
	// match signal
	{
		testOrNot: true,
		name:      "ConstructCaseExprForAdditionalCol",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: &OnJoinCond{
				Expr: &ComparisonExpr{
					Operator:       EQ,
					SubOperator:    EQ,
					Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
					Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
					typeAnnotation: typeAnnotation{Typ: nil},
					fn:             nil,
				},
			},
			MatchList: CondAndOpList{
				{
					Condition: nil,
					Operation: nil,
					IsDelete:  false,
					Sig: &Signal{
						SQLState:    &error1,
						MessageText: &error2,
					},
				},
			},
			MergeOrNot: true,
		},
		inputForCCEA: CCEA{
			tableName:      "em",
			primaryKeyName: "rowid",
		},
		expectForCaseExpr: CaseExpr{
			Expr: nil,
			Whens: []*When{
				{
					Cond: &ComparisonExpr{
						Operator:       EQ,
						SubOperator:    EQ,
						Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
						Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
						typeAnnotation: typeAnnotation{Typ: nil},
						fn:             nil,
					},
					Val: &AdditionalExpr{
						Expr:          nil,
						OperationType: 2,
						Sig: &Signal{
							SQLState:    &error1,
							MessageText: &error2,
						},
						typeAnnotation: typeAnnotation{Typ: types.Int},
					},
				},
				{
					Cond: &AndExpr{
						Left: Datum(DBoolTrue),
						Right: &ComparisonExpr{
							Operator:       IsNotDistinctFrom,
							SubOperator:    EQ,
							Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"rowid", "em", "", ""}},
							Right:          DNull,
							typeAnnotation: typeAnnotation{Typ: nil},
							fn:             nil,
						},
						typeAnnotation: typeAnnotation{Typ: nil},
					},
					Val: &AdditionalExpr{
						Expr:           nil,
						OperationType:  3,
						Sig:            nil,
						typeAnnotation: typeAnnotation{Typ: types.Int},
					},
				},
			},
			Else: &AdditionalExpr{
				Expr:           nil,
				OperationType:  0,
				Sig:            nil,
				typeAnnotation: typeAnnotation{Typ: types.Int},
			},
			typeAnnotation: typeAnnotation{Typ: nil},
		},
	},
	// not match signal
	{
		testOrNot: true,
		name:      "ConstructCaseExprForAdditionalCol",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: &OnJoinCond{
				Expr: &ComparisonExpr{
					Operator:       EQ,
					SubOperator:    EQ,
					Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"employeid", "em", "", ""}},
					Right:          &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
					typeAnnotation: typeAnnotation{Typ: nil},
					fn:             nil,
				},
			},
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: nil,
					ColNames:  nil,
					Sig: &Signal{
						SQLState:    &error1,
						MessageText: &error2,
					},
				},
			},
			MergeOrNot: true,
		},
		inputForCCEA: CCEA{
			tableName:      "em",
			primaryKeyName: "rowid",
		},
		expectForCaseExpr: CaseExpr{
			Expr: nil,
			Whens: []*When{
				{
					Cond: &ComparisonExpr{
						Operator:       IsNotDistinctFrom,
						SubOperator:    EQ,
						Left:           &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"rowid", "em", "", ""}},
						Right:          DNull,
						typeAnnotation: typeAnnotation{Typ: nil},
						fn:             nil,
					},
					Val: &AdditionalExpr{
						Expr:          nil,
						OperationType: 2,
						Sig: &Signal{
							SQLState:    &error1,
							MessageText: &error2,
						},
						typeAnnotation: typeAnnotation{Typ: types.Int},
					},
				},
			},
			Else: &AdditionalExpr{
				Expr:           nil,
				OperationType:  0,
				Sig:            nil,
				typeAnnotation: typeAnnotation{Typ: types.Int},
			},
			typeAnnotation: typeAnnotation{Typ: nil},
		},
	},
	// getUnionColsOfMatch : normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: CondAndOpList{
				{
					Condition: nil,
					Operation: UpdateExprs{
						{
							Tuple: false,
							Names: NameList{
								"name",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
						},
						{
							Tuple: false,
							Names: NameList{
								"salary",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
						{
							Tuple: false,
							Names: NameList{
								"employeid",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					IsDelete: false,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		expectForCols: []Name{"name", "salary", "employeid"},
		expectError:   false,
	},
	// getUnionColsOfMatch : lots of match and normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: CondAndOpList{
				{
					Condition: nil,
					Operation: UpdateExprs{
						{
							Tuple: false,
							Names: NameList{
								"salary",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					IsDelete: false,
					Sig:      nil,
				},
				{
					Condition: nil,
					Operation: UpdateExprs{
						{
							Tuple: false,
							Names: NameList{
								"salary",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
						{
							Tuple: false,
							Names: NameList{
								"name",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
						},
					},
					IsDelete: false,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		expectForCols: []Name{"salary", "name"},
		expectError:   false,
	},
	// getUnionColsOfMatch : needs error
	{
		testOrNot: true,
		name:      "GetUnionColsOfMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: CondAndOpList{
				{
					Condition: nil,
					Operation: UpdateExprs{
						{
							Tuple: false,
							Names: NameList{
								"name",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
						},
						{
							Tuple: false,
							Names: NameList{
								"salary",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
						{
							Tuple: false,
							Names: NameList{
								"name",
							},
							Expr: &UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
						},
					},
					IsDelete: false,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		expectForCols: nil,
		expectError:   true,
	},
	// getUnionColsOfNotMatch : normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: nil,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: []Name{"employeid", "name", "salary"},
		expectError:   false,
	},
	// getUnionColsOfNotMatch : (3)values(3) normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "name", "salary"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: []Name{"employeid", "name", "salary"},
		expectError:   false,
	},
	// getUnionColsOfNotMatch : (2)values(3) error
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "name"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: nil,
		expectError:   true,
	},
	// getUnionColsOfNotMatch : (3)values(1) error
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "name", "salary"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: nil,
		expectError:   true,
	},
	// getUnionColsOfNotMatch : (3 but has wrong name)values(3) error
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "name", "salary"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid2", "name", "salary"},
		},
		expectForCols: nil,
		expectError:   true,
	},
	// getUnionColsOfNotMatch : (2)values(2) normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "name"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: []Name{"employeid", "name"},
		expectError:   false,
	},
	// getUnionColsOfNotMatch : (3 but has different orders)values(3) normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: []Name{"name", "employeid", "salary"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: []Name{"employeid", "name", "salary"},
		expectError:   false,
	},
	// getUnionColsOfNotMatch : (3 but has same names )values(3) error
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "employeid", "salary"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: false,
			colNames:      []Name{"employeid", "name", "salary"},
		},
		expectForCols: nil,
		expectError:   true,
	},
	// getUnionColsOfNotMatch : (3)values(3) and has primary key normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: []Name{"employeid", "name", "salary"},
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: true,
			colNames:      []Name{"employeid", "name", "salary", "rowid"},
		},
		expectForCols: []Name{"employeid", "name", "salary"},
		expectError:   false,
	},
	// getUnionColsOfNotMatch : (nil)values(3) and has primary key normal
	{
		testOrNot: true,
		name:      "GetUnionColsOfNotMatch",
		oc: OnConflict{
			Columns:   nil,
			Exprs:     nil,
			Where:     nil,
			DoNothing: false,
			Condition: joinCondForUnion,
			MatchList: nil,
			NotMatchList: NmCondAndOpList{
				{
					Condition: nil,
					Operation: []Exprs{
						{
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"managerid", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"name", "ma", "", ""}},
							&UnresolvedName{NumParts: 2, Star: false, Parts: NameParts{"salary", "ma", "", ""}},
						},
					},
					ColNames: nil,
					Sig:      nil,
				},
			},
			MergeOrNot: true,
		},
		inputForGUCNM: GUCNM{
			hasPrimaryKey: true,
			colNames:      []Name{"employeid", "name", "salary", "rowid"},
		},
		expectForCols: nil,
		expectError:   true,
	},
}

func TestOnConflict_ConstructCaseExprByColName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, oneCase := range testCase {
		if oneCase.testOrNot && oneCase.name == "ConstructCaseExprByColName" {
			var expr, _ = oneCase.oc.ConstructCaseExprByColName(oneCase.inputForCCEC.colName, oneCase.inputForCCEC.colOrd, oneCase.inputForCCEC.tableName, oneCase.inputForCCEC.matchOrNot, false, nil)
			if expr.String() != oneCase.expectForCaseExpr.String() {
				fmt.Println("result: ", expr.String())
				fmt.Println("expect: ", oneCase.expectForCaseExpr.String())
				t.Fatal("CaseExpr is constructed in a wrong way!")
			} else {
				fmt.Println("CaseExpr Pass!")
			}
		}
	}
}

func TestOnConflict_ConstructCaseExprOfAdditionalCol(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, oneCase := range testCase {
		if oneCase.testOrNot && oneCase.name == "ConstructCaseExprForAdditionalCol" {
			var expr, _ = oneCase.oc.ConstructCaseExprOfAdditionalCol(oneCase.inputForCCEA.tableName, oneCase.inputForCCEA.primaryKeyName)
			if expr.String() != oneCase.expectForCaseExpr.String() {
				fmt.Println("result: ", expr.String())
				fmt.Println("expect: ", oneCase.expectForCaseExpr.String())
				t.Fatal("AdditionalCaseExpr is constructed in a wrong way!")
			} else {
				fmt.Println("AdditionalCaseExpr Pass!")
			}
		}
	}
}

func TestOnConflict_GetUnionColOfMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, oneCase := range testCase {
		if oneCase.testOrNot && oneCase.name == "GetUnionColsOfMatch" {
			nameCols, err := oneCase.oc.GetUnionColListOfMatch()
			// error position
			if err != nil && !oneCase.expectError {
				fmt.Println("result: ", err.Error())
				fmt.Println("expect: there is error in match sql!")
				t.Fatal("occurs error while get union cols of match!")
			} else if !reflect.DeepEqual(oneCase.expectForCols, nameCols) {
				fmt.Println("result: ", nameCols)
				fmt.Println("expect: ", oneCase.expectForCols)
				t.Fatal("GetUnionColListOfMatch() has get wrong cols!")
			} else {
				fmt.Println("GetUnionColListOfMatch Pass!")
			}
		}
	}
}

func TestOnConflict_GetUnionColOfNotMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, oneCase := range testCase {
		if oneCase.testOrNot && oneCase.name == "GetUnionColsOfNotMatch" {
			nameCols, err := oneCase.oc.GetUnionColsOfNotMatch(oneCase.inputForGUCNM.colNames, "rowid", oneCase.inputForGUCNM.hasPrimaryKey)
			// error position
			if err != nil && !oneCase.expectError {
				fmt.Println("result: ", err.Error())
				fmt.Println("expect: occurs error in a normal notMatch sql!")
				t.Fatal("occurs error while get union cols of Not match!")
			} else if !reflect.DeepEqual(oneCase.expectForCols, nameCols) {
				fmt.Println("result: ", nameCols)
				fmt.Println("expect: ", oneCase.expectForCols)
				t.Fatal("GetUnionColListOfNotMatch() has get wrong cols!")
			} else {
				fmt.Println("GetUnionColListOfNotMatch Pass!")
			}
		}
	}
}
