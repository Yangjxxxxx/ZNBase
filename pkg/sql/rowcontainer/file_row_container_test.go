package rowcontainer

import (
	"context"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/sql/sem/tree"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/storage/engine"
	"github.com/znbasedb/znbase/pkg/util/encoding"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/mon"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func TestFileRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempEngine, err := engine.NewTempEngine(ctx, engine.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These orderings assume at least 4 columns.
	numCols := 4
	orderings := []sqlbase.ColumnOrdering{
		{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Ascending,
			},
		},
		{
			sqlbase.ColumnOrderInfo{
				ColIdx:    0,
				Direction: encoding.Descending,
			},
		},
		{
			sqlbase.ColumnOrderInfo{
				ColIdx:    3,
				Direction: encoding.Ascending,
			},
			sqlbase.ColumnOrderInfo{
				ColIdx:    1,
				Direction: encoding.Descending,
			},
			sqlbase.ColumnOrderInfo{
				ColIdx:    2,
				Direction: encoding.Ascending,
			},
		},
	}

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	evalCtx := tree.MakeTestingEvalContext(st)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	fmemoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer diskMonitor.Stop(ctx)

	t.Run("TestFileSorter", func(t *testing.T) {
		numRows := 1024
		for _, ordering := range orderings {

			FileStore := new(Hstore)
			tempName, err := ioutil.TempDir("", "sort_temp_file")
			if err != nil {
				t.Fatal(err)
			}
			err = os.Remove(tempName)
			if err != nil {
				t.Fatal(err)
			}
			FileStore.Init(32, 1, tempName[5:], "./", NewHstoreMemMgr(0))
			if err := FileStore.Open(); err != nil {
				t.Fatal(err)
			}
			rc := FileBackedRowContainer{}
			rc.FileStore = FileStore

			// numRows rows with numCols columns of random types.
			types := sqlbase.RandSortingColumnTypes(rng, numCols)
			rows := sqlbase.RandEncDatumRowsOfTypes(rng, numRows, types)
			func() {
				rc.Init(
					ordering,
					nil,
					&evalCtx,
					tempEngine,
					&memoryMonitor,
					&fmemoryMonitor,
					&diskMonitor,
					0, /* rowCapacity */
				)
				rc.scratchEncRow = make(sqlbase.EncDatumRow, 4)
				d := rc.MakeFileRowContainer(&diskMonitor, types, ordering, tempEngine)
				d.Add(1)
				rc.src = d
				rc.frc = d
				go d.dumpKeyRow(ctx)
				defer d.Close(ctx)
				for i := 0; i < len(rows); i++ {
					if err := d.AddRow(ctx, rows[i]); err != nil {
						t.Fatal(err)
					}
				}
				rc.Sort(ctx)

				// Make another row container that stores all the rows then sort
				// it to compare equality.
				var sortedRows MemRowContainer
				sortedRows.Init(ordering, types, &evalCtx)
				defer sortedRows.Close(ctx)
				for _, row := range rows {
					if err := sortedRows.AddRow(ctx, row); err != nil {
						t.Fatal(err)
					}
				}
				sortedRows.Sort(ctx)

				i := d.NewIterator(ctx)
				defer i.Close()

				numKeysRead := 0
				for i.Rewind(); ; i.Next() {
					if ok, err := i.Valid(); err != nil {
						t.Fatal(err)
					} else if !ok {
						break
					}
					row, err := i.Row()
					if err != nil {
						t.Fatal(err)
					}
					row, err = rc.GetRow(row)
					if err != nil {
						t.Fatal(err)
					}

					// Ensure datum fields are set and no errors occur when
					// decoding.
					for i, encDatum := range row {
						if err := encDatum.EnsureDecoded(&types[i], &d.datumAlloc); err != nil {
							t.Fatal(err)
						}
					}

					// Check sorted order.
					if cmp, err := compareRows(
						types, sortedRows.EncRow(numKeysRead), row, &evalCtx, &d.datumAlloc, ordering,
					); err != nil {
						t.Fatal(err)
					} else if cmp != 0 {
						t.Fatalf(
							"expected %s to be equal to %s",
							row.String(types),
							sortedRows.EncRow(numKeysRead).String(types),
						)
					}
					numKeysRead++
				}
				if numKeysRead != numRows {
					t.Fatalf("expected to read %d keys but only read %d", numRows, numKeysRead)
				}
				if err := FileStore.Close(); err != nil {
					t.Fatal(err)
				}
			}()
			FileStore.HsMgr.Close()
		}
	})
}

func TestHashFileBackedRowContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	tempEngine, err := engine.NewTempEngine(ctx, engine.DefaultStorageEngine, base.DefaultTestTempStorageConfig(st), base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	// These monitors are started and stopped by subtests.
	memoryMonitor := mon.MakeMonitor(
		"test-mem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)
	fmemoryMonitor := mon.MakeMonitor(
		"test-fmem",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		st,
	)

	const numRows = 10
	const numCols = 1
	rows := sqlbase.MakeIntRows(numRows, numCols)
	storedEqColumns := columns{0}
	types := sqlbase.OneIntCol
	ordering := sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}}

	FileStore := new(Hstore)
	tempName, err := ioutil.TempDir("", "sort_temp_file")
	if err != nil {
		t.Fatal(err)
	}
	err = os.Remove(tempName)
	if err != nil {
		t.Fatal(err)
	}
	FileStore.Init(32, 1, tempName[5:], "./", NewHstoreMemMgr(0))
	if err := FileStore.Open(); err != nil {
		t.Fatal(err)
	}
	defer FileStore.HsMgr.Close()
	rc := MakeHashFileBackedRowContainer(nil, &evalCtx, &memoryMonitor, &fmemoryMonitor, &diskMonitor, tempEngine, FileStore)
	err = rc.Init(
		ctx,
		false, /* shouldMark */
		types,
		storedEqColumns,
		true, /*encodeNull */
	)
	if err != nil {
		t.Fatalf("unexpected error while initializing hashDiskBackedRowContainer: %s", err.Error())
	}
	//defer rc.Close(ctx)
	t.Run("HashFileRowContainer", func(t *testing.T) {
		memoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer memoryMonitor.Stop(ctx)
		fmemoryMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer fmemoryMonitor.Stop(ctx)
		diskMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
		defer diskMonitor.Stop(ctx)

		mid := len(rows) / 2
		for i := 0; i < mid; i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		if rc.UsingFile() {
			t.Fatal("unexpectedly using disk")
		}
		func() {
			// We haven't marked any rows, so the unmarked iterator should iterate
			// over all rows added so far.
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows[:mid], &evalCtx, ordering); err != nil {
				t.Fatalf("verifying memory rows failed with: %s", err)
			}
		}()
		if err := rc.SpillToFile(ctx); err != nil {
			t.Fatal(err)
		}
		if !rc.UsingFile() {
			t.Fatal("unexpectedly using memory")
		}
		for i := mid; i < len(rows); i++ {
			if err := rc.AddRow(ctx, rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		func() {
			if _, err := rc.NewBucketIterator(ctx, rows[0], storedEqColumns); err != nil {
				t.Fatal(err)
			}
			i := rc.NewUnmarkedIterator(ctx)
			defer i.Close()
			if err := verifyRows(ctx, i, rows, &evalCtx, ordering); err != nil {
				t.Fatalf("verifying disk rows failed with: %s", err)
			}
		}()
		rc.Close(ctx)
	})
}
