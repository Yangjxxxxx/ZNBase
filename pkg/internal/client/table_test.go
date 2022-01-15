package client_test

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/znbasedb/znbase/pkg/base"
	"github.com/znbasedb/znbase/pkg/internal/client"
	"github.com/znbasedb/znbase/pkg/keys"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
	"github.com/znbasedb/znbase/pkg/testutils/serverutils"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

func TestTablesScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir := "./data/znbasedb/"
	store := base.StoreSpec{Path: dir}
	args := base.TestServerArgs{StoreSpecs: []base.StoreSpec{store}}
	s, _, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(context.TODO())
	db := s.DB()
	endTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	var allDescs []sqlbase.Descriptor
	_ = db.Txn(
		context.TODO(),
		func(ctx context.Context, txn *client.Txn) error {
			var err error
			txn.SetFixedTimestamp(ctx, endTime)

			startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
			endKey := startKey.PrefixEnd()
			rows, err := txn.Scan(ctx, startKey, endKey, 0)
			if err != nil {
				return err
			}
			allDescs = make([]sqlbase.Descriptor, len(rows))
			for i, row := range rows {
				//log.Printf("key: %v\n", row.Key)
				if err := row.ValueProto(&allDescs[i]); err != nil {
					return errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", row.Key)
				}
			}

			return err
		})

	for _, desc := range allDescs {
		table := desc.GetTable()
		if table != nil {
			table.GetParentID() //Database ID
			print("table : %s\n", table.GetName())
		}
	}
}
