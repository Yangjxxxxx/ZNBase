package locatespaceicl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/cli"
	"github.com/znbasedb/znbase/pkg/cli/debug"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/stop"
)

// waiting time for DML operations
const timeForWait = 100

type dmlOperation interface {
	insertIntoTable(t *testing.T) error
	updateToTable(t *testing.T) error
	deleteFromTable(t *testing.T) error
}

var debugCtx struct {
	startKey, endKey  engine.MVCCKey
	values            bool
	sizes             bool
	replicated        bool
	inputFile         string
	ballastSize       base.SizeSpec
	printSystemConfig bool
	maxResults        int64
}

type OnePartition struct {
	//testOrNot
	testorNot bool
	// colOne will store the insert value of the first column, interface{} makes it can be many types
	colOne interface{}
	// colTwo will store the insert value of the second column
	colTwo []string
	// col3 will store the insert value of the third column
	colTree []string
	// condition is the update or delete condition, e.g. col1 = 3
	condition string
	// dataSize is used to enlarge the number of inserted data
	dataSize int
	// types is "delete" or "update" operation choice
	types string
	// set is prepared for update operation
	set string
	// the target table of the insert, update or delete operation
	tableName string
	//queryNodes are the store nodes required to be scanned
	queryNodes []string
	//queryString are what we are going to scan on queryNodes
	queryStrings []string
	//expect[i] means the expect number that query queryString[i] in queryNodes[i]
	expect []int
}

var _ dmlOperation = &OnePartition{}

type dmlTestCase struct {
	// testOrNot implicates this case should be test or not
	testOrNot bool
	// caseName implicates the name of this test case
	caseName string
	// createStmt will implicates the statement of this create table
	createStmt string
	// inserted data and kinds of operations and expected result
	testData []OnePartition
}

var testDmlCase = []dmlTestCase{
	{
		testOrNot: true,
		caseName:  "IntPartition",
		createStmt: "create table %s (col1 int, col2 string,col3 string, primary key (col1, col2)) " +
			"partition by list(col1) (" +
			"partition p1 values in(3) locate in (sp0)," +
			"partition p2 values in(6) locate in (sp1)," +
			"partition p3 values in(9) locate in (sp2))",

		testData: []OnePartition{
			{
				testorNot: true,
				tableName: "IntPartition",
				types:     "insert",
				colOne:    []int{3, 6, 9},
				colTwo:    []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"},
				dataSize:  300,
				colTree:   []string{"a"},
				queryNodes: []string{"store1", "store2", "store3", "store1", "store2", "store3", "store1", "store2", "store3", "store4", "store5", "store6", "store4",
					"store5", "store6", "store4", "store5", "store6", "store7", "store8", "store9", "store7", "store8", "store9", "store7", "store8", "store9"},
				queryStrings: []string{"partition3", "partition3", "partition3", "partition6", "partition6", "partition6", "partition9", "partition9", "partition9",
					"partition3", "partition3", "partition3", "partition6", "partition6", "partition6", "partition9", "partition9", "partition9",
					"partition3", "partition3", "partition3", "partition6", "partition6", "partition6", "partition9", "partition9", "partition9"},
				expect: []int{3300, 3300, 3300, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 3300, 3300, 3300, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 3300, 3300, 3300},
			},
			{
				testorNot: false,
				tableName: "IntPartition",
				//update will let the col2 to be 'new' + col2
				types:      "update",
				condition:  "col1 = 3",
				set:        "col1 = 6",
				queryNodes: []string{"store1", "store2", "store3", "store1", "store2", "store3", "store4", "store5", "store6", "store7", "store8", "store9"},
				queryStrings: []string{"partition3", "partition3", "partition3", "newpartition3", "newpartition3", "newpartition3", "newpartition3", "newpartition3", "newpartition3",
					"newpartition3", "newpartition3", "newpartition3"},
				expect: []int{0, 0, 0, 0, 0, 0, 3300, 3300, 3300, 0, 0, 0},
			},
			{
				testorNot:    false,
				tableName:    "IntPartition",
				types:        "delete",
				condition:    "col1 = 9",
				queryNodes:   []string{"store7", "store8", "store9", "store1", "store2", "store3", "store4", "store5", "store6"},
				queryStrings: []string{"partition9", "partition9", "partition9", "partition9", "partition9", "partition9", "partition9", "partition9", "partition9"},
				expect:       []int{0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
	},

	{
		testOrNot: false,
		caseName:  "IntPartition_by2",
		createStmt: "create table %s (col1 int, col3 string, col2 string, primary key (col1, col3, col2))" +
			"partition by list(col1, col3) (" +
			"partition p1 values in ((3,'value3a'),(3,'value3b'),(6,'value6a')) locate in (sp0)," +
			"partition p2 values in ((6,'value6b'),(6,'value6c'),(9,'value9a')) locate in (sp1)," +
			"partition p3 values in ((9,'value9b'),(9,'value9c'),(3,'value3c')) locate in (sp2))",
		testData: []OnePartition{
			{
				testorNot: true,
				tableName: "IntPartition_by2",
				types:     "insert",
				colOne:    []int{3, 6, 9},
				colTwo:    []string{"a", "b", "c", "d", "e", "f", "g"},
				colTree:   []string{"a", "b", "c"},
				dataSize:  160,
				queryNodes: []string{"store1", "store1", "store1", "store2", "store2", "store2", "store3", "store3", "store3",
					"store4", "store4", "store4", "store5", "store5", "store5", "store6", "store6", "store6",
					"store7", "store7", "store7", "store8", "store8", "store8", "store9", "store9", "store9"},
				queryStrings: []string{"value3a", "value3b", "value6a", "value3a", "value3b", "value6a", "value3a", "value3b", "value6a",
					"value6b", "value6c", "value9a", "value6b", "value6c", "value9a", "value6b", "value6c", "value9a",
					"value9b", "value9c", "value3c", "value9b", "value9c", "value3c", "value9b", "value9c", "value3c"},
				expect: []int{1120, 1120, 1120, 1120, 1120, 1120, 1120, 1120, 1120,
					1120, 1120, 1120, 1120, 1120, 1120, 1120, 1120, 1120,
					1120, 1120, 1120, 1120, 1120, 1120, 1120, 1120, 1120},
			},
			{
				testorNot: true,
				tableName: "IntPartition_by2",
				types:     "update",
				condition: "col1 = 6 and col3 = 'value6c'",
				set:       "col1 = 3, col3 = 'value3c'",
				queryNodes: []string{"store1", "store1", "store2", "store2", "store3", "store3",
					"store4", "store4", "store5", "store5", "store6", "store6", "store4", "store5", "store6",
					"store7", "store8", "store9"},
				// "/3/\"va" means to the col1 == 3 and the col3 is "va?"
				queryStrings: []string{"/3/\"va", "/6/\"va", "/3/\"va", "/6/\"va", "/3/\"va", "/6/\"va",
					"/9/\"va", "/6/\"va", "/9/\"va", "/6/\"va", "/9/\"va", "/6/\"va", "value6c", "value6c", "value6c",
					"value3c", "value3c", "value3c"},
				expect: []int{2240, 1120, 2240, 1120, 2240, 1120,
					1120, 1120, 1120, 1120, 1120, 1120, 0, 0, 0,
					2240, 2240, 2240},
			},

			{
				testorNot: true,
				tableName: "IntPartition_by2",
				types:     "delete",
				condition: "col3 = 'value3c' or col1 = 9",
				queryNodes: []string{"store1", "store2", "store3", "store1", "store2", "store3",
					"store4", "store5", "store6", "store4", "store5", "store6",
					"store7", "store8", "store9"},
				queryStrings: []string{"value3", "value3", "value3", "value6", "value6", "value6",
					"value6b", "value6b", "value6b", "value9", "value9", "value9",
					"value", "value", "value"},
				expect: []int{2240, 2240, 2240, 1120, 1120, 1120,
					1120, 1120, 1120, 0, 0, 0,
					0, 0, 0},
			},
		},
	},

	{
		testOrNot: false,
		caseName:  "IntSubPartition",
		createStmt: "create table %s (col1 int, col2 string,col3 string, primary key (col1, col3, col2)) " +
			"partition by list(col1) (" +
			"partition p1 values in(3) locate in sp0 partition by list(col3) (" +
			"partition p11 values in('value3a') locate in (sp1)," +
			"partition p12 values in('value3b') locate in (sp2))," +
			"partition p2 values in(6) locate in (sp1) partition by list(col3) (" +
			"partition p21 values in('value6a') locate in (sp0)," +
			"partition p22 values in('value6b') locate in (sp2))," +
			"partition p3 values in(9) locate in (sp2) partition by list(col3) (" +
			"partition p31 values in('value9a') locate in (sp0)," +
			"partition p32 values in('value9b') locate in (sp1)))",

		testData: []OnePartition{
			{
				testorNot: true,
				tableName: "IntSubPartition",
				types:     "insert",
				colOne:    []int{3, 6, 9},
				colTwo:    []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
				dataSize:  160,
				colTree:   []string{"a", "b"},
				queryNodes: []string{"store4", "store5", "store6", "store7", "store8", "store9", "store1", "store2", "store3", "store7", "store8", "store9", "store1",
					"store2", "store3", "store4", "store5", "store6"},
				queryStrings: []string{"value3a", "value3a", "value3a", "value3b", "value3b", "value3b", "value6a", "value6a", "value6a", "value6b", "value6b", "value6b", "value9a", "value9a", "value9a", "value9b", "value9b", "value9b"},
				expect:       []int{1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600, 1600},
			},
			{
				testorNot: true,
				tableName: "IntSubPartition",
				//update will let the col2 to be 'new' + col2
				types:        "update",
				condition:    "col1 = 3",
				set:          "col1 = 6",
				queryNodes:   []string{"store1", "store2", "store3", "store4", "store5", "store6", "store7", "store8", "store9", "store4", "store5", "store6", "store4", "store5", "store6", "store4", "store5", "store6"},
				queryStrings: []string{"value3a", "value3a", "value3a", "value3a", "value3a", "value3a", "value3b", "value3b", "value3b", "value3b", "value3b", "value3b", "value6a", "value6a", "value6a", "value6b", "value6b", "value6b"},

				expect: []int{0, 0, 0, 1600, 1600, 1600, 0, 0, 0, 1600, 1600, 1600, 0, 0, 0, 0, 0, 0},
			},
			{
				testorNot:    true,
				tableName:    "IntSubPartition",
				types:        "delete",
				condition:    "col1 = 6",
				queryNodes:   []string{"store4", "store5", "store6", "store4", "store5", "store6", "store1", "store2", "store3", "store7", "store8", "store9"},
				queryStrings: []string{"value3a", "value3a", "value3a", "value3b", "value3b", "value3b", "value6a", "value6a", "value6a", "value6b", "value6b", "value6b"},
				expect:       []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
	},

	{
		testOrNot: false,
		caseName:  "IntPartition_listAny",
		createStmt: "create table %s (col1 int, col2 string,col3 string, primary key (col1, col2)) " +
			"partition by list(col1) (" +
			"partition p1 values in(3,4) locate in (sp0)," +
			"partition p2 values in(6,7,8) locate in (sp1)," +
			"partition p3 values in(1,2,5,9) locate in (sp2))",
		testData: []OnePartition{
			{
				testorNot: true,
				types:     "insert",
				tableName: "IntPartition_listAny",
				colOne:    []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
				colTwo:    []string{"a", "b", "c", "d", "e", "f"},
				colTree:   []string{"v"},
				dataSize:  200,
				queryNodes: []string{"store1", "store1", "store2", "store2", "store3", "store3",
					"store4", "store4", "store4", "store5", "store5", "store5", "store6", "store6", "store6",
					"store7", "store7", "store7", "store7", "store8", "store8", "store8", "store8", "store9", "store9", "store9", "store9"},
				queryStrings: []string{"partition3", "partition4", "partition3", "partition4", "partition3", "partition4",
					"partition6", "partition7", "partition8", "partition6", "partition7", "partition8", "partition6", "partition7", "partition8",
					"partition1", "partition2", "partition5", "partition9", "partition1", "partition2", "partition5", "partition9",
					"partition1", "partition2", "partition5", "partition9"},
				expect: []int{1200, 1200, 1200, 1200, 1200, 1200,
					1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200,
					1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200, 1200},
			},
			{
				testorNot: true,
				types:     "update",
				tableName: "IntPartition_listAny",
				condition: "col1 = 9  or col1 = 5",
				set:       "col1 = col1 - 1",
				queryNodes: []string{"store1", "store1", "store2", "store2", "store3", "store3",
					"store4", "store4", "store5", "store5", "store6", "store6",
					"store7", "store7", "store8", "store8", "store9", "store9"},
				queryStrings: []string{"newpartition9", "newpartition5", "newpartition9", "newpartition5", "newpartition9", "newpartition5",
					"newpartition9", "newpartition5", "newpartition9", "newpartition5", "newpartition9", "newpartition5",
					"newpartition9", "newpartition5", "newpartition9", "newpartition5", "newpartition9", "newpartition5"},
				expect: []int{0, 1200, 0, 1200, 0, 1200,
					1200, 0, 1200, 0, 1200, 0,
					0, 0, 0, 0, 0, 0},
			},
			{
				testorNot:    true,
				types:        "delete",
				tableName:    "IntPartition_listAny",
				condition:    "col2 like 'new%'",
				queryNodes:   []string{"store1", "store2", "store3", "store4", "store5", "store6"},
				queryStrings: []string{"newpartition5", "newpartition5", "newpartition5", "newpartition9", "newpartition9", "newpartition9"},
				expect:       []int{0, 0, 0, 0, 0, 0},
			},
		},
	},

	{
		testOrNot: false,
		caseName:  "IntPartition_listRange",
		createStmt: "create table %s (col1 int, col2 string,col3 string, primary key (col3, col1, col2)) " +
			"partition by list(col3) (" +
			"partition p1 values in('value3a','value1a','value1b','value3b','value2a','value2b') locate in (sp0) partition by range(col1) (" +
			"partition p11 values from(1) to (3) locate in (sp1)," +
			"partition p12 values from(3) to (4) locate in (sp2))," +
			"partition p2 values in('value4a','value5a','value4b','value5b','value6a','value6b') locate in (sp1) partition by range(col1) (" +
			"partition p21 values from(4) to (6) locate in (sp0)," +
			"partition p22 values from(6) to (7) locate in (sp2))," +
			"partition p3 values in('value7a','value8a','value7b','value8b','value9a','value9b') locate in (sp2) partition by range(col1) (" +
			"partition p31 values from(7) to (9) locate in (sp0)," +
			"partition p32 values from(9) to (10) locate in (sp1)))",
		testData: []OnePartition{
			{
				testorNot:  true,
				tableName:  "IntPartition_listRange",
				types:      "insert",
				dataSize:   130,
				colOne:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
				colTwo:     []string{"a", "b", "c", "d"},
				colTree:    []string{"a", "b"},
				queryNodes: []string{"store1", "store1", "store1", "store1", "store2", "store2", "store2", "store2", "store3", "store3", "store3", "store3"},
				queryStrings: []string{"partition4", "partition5", "partition7", "partition8", "partition4", "partition5", "partition7", "partition8",
					"partition4", "partition5", "partition7", "partition8"},
				expect: []int{1040, 1040, 1040, 1040, 1040, 1040, 1040, 1040, 1040, 1040, 1040, 1040},
			},
			{
				testorNot:  true,
				tableName:  "IntPartition_listRange",
				types:      "update",
				condition:  "col1 = 6 and col3 = 'value6b'",
				set:        "col3 = 'value3b'",
				queryNodes: []string{"store1", "store2", "store3", "store4", "store5", "store6", "store7", "store8", "store9"},
				queryStrings: []string{"newpartition6", "newpartition6", "newpartition6", "newpartition6", "newpartition6", "newpartition6",
					"newpartition6", "newpartition6", "newpartition6"},
				expect: []int{520, 520, 520, 0, 0, 0, 0, 0, 0},
			},
			{
				testorNot:  true,
				tableName:  "IntPartition_listRange",
				types:      "delete",
				condition:  "col1 <> 9 and col2 like 'partition%'",
				queryNodes: []string{"store1", "store2", "store3", "store4", "store4", "store5", "store5", "store6", "store6", "store7", "store8", "store9"},
				queryStrings: []string{"partition", "partition", "partition",
					"partition", "partition9", "partition", "partition9", "partition", "partition9",
					"partition", "partition", "partition"},
				expect: []int{520, 520, 520, 1040, 1040, 1040, 1040, 1040, 1040, 0, 0, 0},
			},
		},
	},

	{
		testOrNot: false,
		caseName:  "IntPartitionByRange",
		createStmt: "create table %s (col1 int, col2 string,col3 string, primary key (col1, col2)) " +
			"partition by range(col1) (" +
			"partition p1 values FROM (1) TO (3) locate in (sp0)," +
			"partition p2 values FROM (3) TO (5) locate in (sp1)," +
			"partition p3 values FROM (5) TO (7) locate in (sp2))",

		testData: []OnePartition{
			{
				testorNot: true,
				tableName: "IntPartitionByRange",
				types:     "insert",
				colOne:    []int{1, 2, 3, 4, 5, 6},
				colTwo:    []string{"a", "b", "c", "d", "e"},
				dataSize:  300,
				colTree:   []string{"a"},
				queryNodes: []string{"store1", "store2", "store3", "store1", "store2", "store3", "store4", "store5", "store6", "store4", "store5", "store6",
					"store7", "store8", "store9", "store7", "store8", "store9"},
				queryStrings: []string{"partition1", "partition1", "partition1", "partition2", "partition2", "partition2", "partition3", "partition3", "partition3",
					"partition4", "partition4", "partition4", "partition5", "partition5", "partition5", "partition6", "partition6", "partition6"},
				expect: []int{1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500, 1500},
			},
			{
				testorNot: true,
				tableName: "IntPartitionByRange",
				//update will let the col2 to be 'new' + col2
				types:        "update",
				condition:    "col1 = 1 OR col1 = 2",
				set:          "col1 = 3",
				queryNodes:   []string{"store1", "store2", "store3", "store4", "store5", "store6", "store4", "store5", "store6", "store1", "store2", "store3"},
				queryStrings: []string{"partition1", "partition1", "partition1", "partition1", "partition1", "partition1", "partition2", "partition2", "partition2", "partition2", "partition2", "partition2"},
				expect:       []int{0, 0, 0, 1500, 1500, 1500, 1500, 1500, 1500, 0, 0, 0},
			},
			{
				testorNot:    true,
				tableName:    "IntPartitionByRange",
				types:        "delete",
				condition:    "col1 = 3",
				queryNodes:   []string{"store4", "store5", "store6", "store4", "store5", "store6", "store4", "store5", "store6", "store4", "store5", "store6"},
				queryStrings: []string{"partition1", "partition1", "partition1", "partition2", "partition2", "partition2", "partition3", "partition3", "partition3", "partition4", "partition4", "partition4"},
				expect:       []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 1500, 1500, 1500},
			},
		},
	},

	{
		testOrNot: false,
		caseName:  "StringPartition",
		createStmt: "create table %s (col1 string, col2 string, col3 string, primary key (col1, col2)) " +
			"partition by list(col1) (" +
			"partition p1 values in('apple') locate in (sp0)," +
			"partition p2 values in('tesla') locate in (sp1)," +
			"partition p3 values in('ali') locate in (sp2) )",

		testData: []OnePartition{
			{
				testorNot:    true,
				tableName:    "StringPartition",
				types:        "insert",
				colOne:       []string{"apple", "tesla", "ali"},
				colTwo:       []string{"jobs", "musk", "mayun"},
				dataSize:     1000,
				colTree:      []string{"666"},
				queryNodes:   []string{"store1", "store2", "store3", "store4", "store5", "store6", "store7", "store8", "store9"},
				queryStrings: []string{"apple", "apple", "apple", "tesla", "tesla", "tesla", "ali", "ali", "ali"},
				expect:       []int{3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000, 3000},
			},
			{
				testorNot: true,
				tableName: "StringPartition",
				//update will let the col2 to be 'new' + col2
				types:        "update",
				condition:    "col1 = 'apple'",
				set:          "col1 = 'tesla'",
				queryNodes:   []string{"store1", "store2", "store3", "store4", "store5", "store6"},
				queryStrings: []string{"apple", "apple", "apple", "apple", "apple", "apple"},
				expect:       []int{0, 0, 0, 3000, 3000, 3000},
			},
			{
				testorNot:    true,
				tableName:    "StringPartition",
				types:        "delete",
				condition:    "col1 = 'tesla'",
				queryNodes:   []string{"store4", "store5", "store6"},
				queryStrings: []string{"tesla", "tesla", "tesla"},
				expect:       []int{0, 0, 0},
			},
		},
	},
}

// createTableTask will do the create table job
func createTableTask(testCase dmlTestCase, t *testing.T) error {
	t.Log("Create Table task start!!")
	var r []interface{}
	r = append(r, testCase.caseName)
	createStmt := fmt.Sprintf(testCase.createStmt, r...)
	sqlDB.Exec(t, createStmt)
	return nil
}

// queryRangeResultTask run an query show experimental_ranges from table test
/*show experimental_ranges from table test;
start_key | end_key | range_id | replicas | lease_holder
+-----------+---------+----------+----------+--------------+
NULL      | /100    |       91 | {1,2,3}  |            3
/100      | /101    |       92 | {1,2,3}  |            1
/101      | NULL    |       93 | {1,2,3}  |            1
*/
//func queryRangeResultTask(testCase dmlTestCase, t *testing.T) error {
//	t.Log("query Range Result task start!!")
//	return nil
//}

// used to print data stored in one node.
func printKey(kv engine.MVCCKeyValue) (bool, error) {
	fmt.Printf("%s %s: ", kv.Key.Timestamp, kv.Key.Key)
	if debugCtx.sizes {
		fmt.Printf(" %d %d", len(kv.Key.Key), len(kv.Value))
	}
	fmt.Printf("\n")
	return false, nil
}

// check whether data is located in the correct node or not.
func queryStoreResult(args []string, content string, t *testing.T, key string) (int, error) {
	var db engine.Engine
	var err error
	var response string
	debugCtx.startKey = engine.NilKey
	debugCtx.endKey = engine.MVCCKeyMax
	debugCtx.values = true
	debugCtx.sizes = true
	debugCtx.replicated = true
	debugCtx.inputFile = ""
	debugCtx.printSystemConfig = false
	debugCtx.maxResults = 1000
	debugCtx.ballastSize = base.SizeSpec{}

	var GOPATH = os.Getenv("GOPATH")

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	printer := printKey
	var contentCount = 0
	var valueTmp = ""
	if debugCtx.values {
		printer = func(kv engine.MVCCKeyValue) (bool, error) {
			response = debug.SprintKeyValue(kv)
			tmp := strings.Split(response, " ")
			if response[0] != '0' && strings.Contains(tmp[1], content) {
				if tmp[1] == valueTmp {
				} else {
					valueTmp = tmp[1]
					keyTmp := "/Table/" + key
					if tmp[3] != "\"\"" && strings.Contains(tmp[1], keyTmp) && tmp[1][len(tmp[1])-1] == '0' {
						contentCount++
					}
				}
			}
			return false, nil
		}
	}
	for i := 0; i < len(args); i++ {
		t.Run(GOPATH+"/tmp/"+args[i], func(t *testing.T) {
			path := GOPATH + "/tmp/" + args[i]
			db, err = cli.OpenExistingStore(path, stopper, true /* readOnly */)
			if err != nil {
				//return err, 0
				t.Log("open DB error !")
			}

			res := db.Iterate(debugCtx.startKey, debugCtx.endKey, printer)
			if res != nil {
				//return res, 0
				t.Log("Iterate DB error !")
			}
		})
	}
	return contentCount, nil
}

// insert data to table based on the input in testData
func (s *OnePartition) insertIntoTable(t *testing.T) error {
	t.Log("insert table task start!!")
	var stmt string
	// check the data type of colOne
	if reflect.TypeOf(s.colOne).String() == "[]string" {
		stmt = "insert into %s (col1, col2, col3) values ('%s', '%s%s','%s')"
		coloneNew := s.colOne.([]string)
		var add string
		for _, v := range coloneNew {
			for _, v2 := range s.colTwo {
				for _, v3 := range s.colTree {
					add = ""
					for j := 0; j < s.dataSize; j++ {
						var r []interface{}
						r = append(r, s.tableName)
						r = append(r, v)
						r = append(r, v)
						add += v2
						r = append(r, add)
						r = append(r, v3)
						insertStmt := fmt.Sprintf(stmt, r...)
						sqlDB.Exec(t, insertStmt)
					}
				}
			}
		}
	} else if reflect.TypeOf(s.colOne).String() == "[]int" {
		stmt = "insert into %s (col1, col2, col3) values (%d, 'partition%d%s','value%d%s')"
		coloneNew := s.colOne.([]int)
		var add string
		for _, v := range coloneNew {
			for _, v2 := range s.colTwo {
				for _, v3 := range s.colTree {
					add = ""
					for j := 0; j < s.dataSize; j++ {
						var r []interface{}
						r = append(r, s.tableName)
						r = append(r, v)
						r = append(r, v)
						add += v2
						r = append(r, add)
						r = append(r, v)
						r = append(r, v3)
						insertStmt := fmt.Sprintf(stmt, r...)
						sqlDB.Exec(t, insertStmt)
					}
				}
			}
		}
	}

	// start query result on store nodes
	time.Sleep(time.Second * timeForWait)
	testutils.SucceedsSoon(t, func() error {
		//out the system table
		name := "%s"
		name = fmt.Sprintf(name, strings.ToLower(s.tableName))
		tableKey := querySystemTable(t, name)

		for i := 0; i < len(s.queryNodes); i++ {
			var ss []string
			ss = append(ss, s.queryNodes[i])
			value, _ := queryStoreResult(ss, s.queryStrings[i], t, tableKey)
			if value != s.expect[i] {
				fmt.Printf("it should be %d in Node %s to query %s but it is %d .....in insert function,in table %s, table key %s\n", s.expect[i], s.queryNodes[i],
					s.queryStrings[i], value, s.tableName, tableKey)
				return errors.Errorf("there is some query wrong in insert function")
			}
		}
		t.Log("Insert test OK!")
		return nil
	})
	return nil
}

//query the table_key
func querySystemTable(t *testing.T, TableName string) string {
	stmt := "select table_id from zbdb_internal.tables where name = '%s';"
	stmt = fmt.Sprintf(stmt, TableName)
	rows, err := db.Query(stmt)
	if err != nil {
		t.Fatal("function querySystemTable error 1")
		return "error"
	}
	defer rows.Close()

	//var traceLines []string
	var traceLine gosql.NullString
	if rows.Next() {
		if err := rows.Scan(&traceLine); err != nil {
			t.Fatal("function querySystemTable error 2")
			return "error"
		}
		return traceLine.String
	}
	t.Fatal("function querySystemTable error 3")
	return "error"
}

// update data in table based on the 'condition' and 'set' in testData
func (s *OnePartition) updateToTable(t *testing.T) error {
	t.Log("update table task start!!")
	var stmt = "update %s set %s, col2 =concat('new',col2) " + "where %s"
	var r []interface{}
	r = append(r, s.tableName)
	r = append(r, s.set)
	r = append(r, s.condition)
	updateStmt := fmt.Sprintf(stmt, r...)
	sqlDB.Exec(t, updateStmt)
	time.Sleep(time.Second * timeForWait)

	testutils.SucceedsSoon(t, func() error {
		//out the system table key
		name := "%s"
		name = fmt.Sprintf(name, strings.ToLower(s.tableName))
		tableKey := querySystemTable(t, name)

		for i := 0; i < len(s.queryNodes); i++ {
			var ss []string
			ss = append(ss, s.queryNodes[i])
			value, _ := queryStoreResult(ss, s.queryStrings[i], t, tableKey)
			if value != s.expect[i] {
				fmt.Printf("it should be %d in Node %s to query %s but it is %d .....in update function,in table %s, table key %s\n", s.expect[i], s.queryNodes[i],
					s.queryStrings[i], value, s.tableName, tableKey)
				return errors.Errorf("there is some query wrong in update function")
			}
		}
		t.Log("update is OK")
		return nil
	})
	return nil
}

// delete data in table based on the 'condition' in testData
func (s *OnePartition) deleteFromTable(t *testing.T) error {
	t.Log("delete table task start!!")
	var stmt = "delete from %s " + "where %s"
	var r []interface{}
	r = append(r, s.tableName)
	r = append(r, s.condition)
	deleteStmt := fmt.Sprintf(stmt, r...)
	sqlDB.Exec(t, deleteStmt)
	time.Sleep(time.Second * timeForWait)
	testutils.SucceedsSoon(t, func() error {
		//out the system table key
		name := "%s"
		name = fmt.Sprintf(name, strings.ToLower(s.tableName))
		tableKey := querySystemTable(t, name)

		for i := 0; i < len(s.queryNodes); i++ {
			var ss []string
			ss = append(ss, s.queryNodes[i])
			value, _ := queryStoreResult(ss, s.queryStrings[i], t, tableKey)
			if value != s.expect[i] {
				fmt.Printf("it should be %d in Node %s to query %s but it is %d .....in delete function,in table %s, table key %s\n", s.expect[i], s.queryNodes[i],
					s.queryStrings[i], value, s.tableName, tableKey)
				return errors.Errorf("there is some query wrong in delete function")
			}
		}
		t.Log("Delete test OK!")
		return nil
	})
	return nil
}

// set spaces and nodes, initialize
func dmlInit(t *testing.T) {
	t.Log("DML locatespace test init!!")
	ctx := context.Background()
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"}
	spaces := [][]string{
		{"sp0"},
		{"sp0"},
		{"sp0"},
		{"sp1"},
		{"sp1"},
		{"sp1"},
		{"sp2"},
		{"sp2"},
		{"sp2"},
	}
	// test server store path
	var GOPATH = os.Getenv("GOPATH")
	t.Log("clear the store ----")
	storeLocation := GOPATH + "/tmp"
	if strings.EqualFold(storeLocation, "/tmp") {
		t.Fatal("GOPATH not exist!")
	}
	_ = os.RemoveAll(storeLocation)
	storePath := []string{
		storeLocation + "/store1",
		storeLocation + "/store2",
		storeLocation + "/store3",
		storeLocation + "/store4",
		storeLocation + "/store5",
		storeLocation + "/store6",
		storeLocation + "/store7",
		storeLocation + "/store8",
		storeLocation + "/store9",
	}
	db, sqlDB, tc, cleanup = setupTestCluster(ctx, t, spaces, nodes, storePath, len(nodes))
	t.Log("Cluster has been setup!!")
}

// start unit test
func TestDmlOperation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dmlInit(t)
	defer cleanup()

	t.Log("DML operation test start!!")
	for _, oneCase := range testDmlCase {
		if oneCase.testOrNot {
			t.Run(oneCase.caseName, func(t *testing.T) {
				// first create table
				if err := createTableTask(oneCase, t); err != nil {
					t.Fatal("create table job failed!!")
				}

				// second operate data of table, every time after operation, use queryStoreResult to check whether this operation returns the correct result or not
				// this value of dmlType is never used (SA4006)
				// dmlType := oneCase.dataDml
				for _, oneDML := range oneCase.testData {
					var dmlType dmlOperation = &oneDML
					if oneDML.testorNot {
						if oneDML.types == "insert" && oneDML.tableName == oneCase.caseName {
							if err := dmlType.insertIntoTable(t); err != nil {
								t.Fatal("Insert into Table error!!")
							}
						} else if oneDML.types == "update" && oneDML.tableName == oneCase.caseName {
							if err := dmlType.updateToTable(t); err != nil {
								t.Fatal("update into Table error!!")
							}
						} else if oneDML.types == "delete" && oneDML.tableName == oneCase.caseName {
							if err := dmlType.deleteFromTable(t); err != nil {
								t.Fatal("delete from table error!")
							}
						} else {
							t.Fatal("dml testCase is  error!!")
						}
					}
				}
			})
		}
	}
}
