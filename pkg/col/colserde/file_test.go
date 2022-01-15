// Copyright 2019  The Cockroach Authors.

package colserde_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/col/coldata"
	"github.com/znbasedb/znbase/pkg/col/colserde"
	"github.com/znbasedb/znbase/pkg/col/coltypes"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
)

func TestFileRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typs, b := randomBatch(testAllocator)

	t.Run(`mem`, func(t *testing.T) {
		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := copyBatch(b)

		var buf bytes.Buffer
		s, err := colserde.NewFileSerializer(&buf, typs)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(b))
		require.NoError(t, s.Finish())

		// Parts of the deserialization modify things (null bitmaps) in place, so
		// run it twice to make sure those modifications don't leak back to the
		// buffer.
		for i := 0; i < 2; i++ {
			func() {
				roundtrip := coldata.NewMemBatchWithSize(nil, 0)
				d, err := colserde.NewFileDeserializerFromBytes(buf.Bytes())
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				require.Equal(t, typs, d.Typs())
				require.Equal(t, 1, d.NumBatches())
				require.NoError(t, d.GetBatch(0, roundtrip))

				assertEqualBatches(t, original, roundtrip)
			}()
		}
	})

	t.Run(`disk`, func(t *testing.T) {
		dir, cleanup := testutils.TempDir(t)
		defer cleanup()
		path := filepath.Join(dir, `rng.arrow`)

		// Make a copy of the original batch because the converter modifies and
		// casts data without copying for performance reasons.
		original := copyBatch(b)

		f, err := os.Create(path)
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		s, err := colserde.NewFileSerializer(f, typs)
		require.NoError(t, err)
		require.NoError(t, s.AppendBatch(b))
		require.NoError(t, s.Finish())
		require.NoError(t, f.Sync())

		// Parts of the deserialization modify things (null bitmaps) in place, so
		// run it twice to make sure those modifications don't leak back to the
		// file.
		for i := 0; i < 2; i++ {
			func() {
				roundtrip := coldata.NewMemBatchWithSize(nil, 0)
				d, err := colserde.NewFileDeserializerFromPath(path)
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				require.Equal(t, typs, d.Typs())
				require.Equal(t, 1, d.NumBatches())
				require.NoError(t, d.GetBatch(0, roundtrip))

				assertEqualBatches(t, original, roundtrip)
			}()
		}
	})
}

func TestFileIndexing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numInts = 10
	typs := []coltypes.T{coltypes.Int64}

	var buf bytes.Buffer
	s, err := colserde.NewFileSerializer(&buf, typs)
	require.NoError(t, err)

	for i := 0; i < numInts; i++ {
		b := coldata.NewMemBatchWithSize(typs, 1)
		b.SetLength(1)
		b.ColVec(0).Int64()[0] = int64(i)
		require.NoError(t, s.AppendBatch(b))
	}
	require.NoError(t, s.Finish())

	d, err := colserde.NewFileDeserializerFromBytes(buf.Bytes())
	require.NoError(t, err)
	defer func() { require.NoError(t, d.Close()) }()
	require.Equal(t, typs, d.Typs())
	require.Equal(t, numInts, d.NumBatches())
	for batchIdx := numInts - 1; batchIdx >= 0; batchIdx-- {
		b := coldata.NewMemBatchWithSize(nil, 0)
		require.NoError(t, d.GetBatch(batchIdx, b))
		require.Equal(t, uint16(1), b.Length())
		require.Equal(t, 1, b.Width())
		require.Equal(t, coltypes.Int64, b.ColVec(0).Type())
		require.Equal(t, int64(batchIdx), b.ColVec(0).Int64()[0])
	}
}
