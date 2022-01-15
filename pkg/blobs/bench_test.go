package blobs

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/znbasedb/errors"
	"github.com/znbasedb/znbase/pkg/blobs/blobspb"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/rpc"
	"github.com/znbasedb/znbase/pkg/rpc/nodedialer"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util"
	"github.com/znbasedb/znbase/pkg/util/hlc"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/netutil"
	"github.com/znbasedb/znbase/pkg/util/stop"
)

type benchmarkTestCase struct {
	localNodeID       roachpb.NodeID
	remoteNodeID      roachpb.NodeID
	localExternalDir  string
	remoteExternalDir string

	blobClient BlobClient
	fileSize   int64
	fileName   string
}

func writeLargeFile(t testing.TB, file string, size int64) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		t.Fatal(err)
	}
	content := make([]byte, size)
	err = ioutil.WriteFile(file, content, 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkStreamingReadFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(context.TODO(), remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          1 << 30, // 1 GB
		fileName:          "test/largefile.csv",
	}
	benchmarkStreamingReadFile(b, params)
}

func benchmarkStreamingReadFile(b *testing.B, tc *benchmarkTestCase) {
	writeLargeFile(b, filepath.Join(tc.remoteExternalDir, tc.fileName), tc.fileSize)
	writeTo := localStorage{ExternalIODir: tc.localExternalDir}
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	for i := 0; i < b.N; i++ {
		reader, err := tc.blobClient.ReadFile(context.TODO(), tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		err = writeTo.WriteFile(tc.fileName, reader)
		if err != nil {
			b.Fatal(err)
		}
		stat, err := writeTo.Stat(tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		if stat.Filesize != tc.fileSize {
			b.Fatal("incorrect number of bytes written")
		}
	}
}

func BenchmarkStreamingWriteFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(context.TODO(), remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          1 << 30, // 1 GB
		fileName:          "test/largefile.csv",
	}
	benchmarkStreamingWriteFile(b, params)
}

func benchmarkStreamingWriteFile(b *testing.B, tc *benchmarkTestCase) {
	content := make([]byte, tc.fileSize)
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	for i := 0; i < b.N; i++ {
		err := tc.blobClient.WriteFile(context.TODO(), tc.fileName, bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func createTestResources(t testing.TB) (string, string, *stop.Stopper, func()) {
	localExternalDir, cleanupFn := testutils.TempDir(t)
	remoteExternalDir, cleanupFn2 := testutils.TempDir(t)
	stopper := stop.NewStopper()
	return localExternalDir, remoteExternalDir, stopper, func() {
		cleanupFn()
		cleanupFn2()
		stopper.Stop(context.Background())
		leaktest.AfterTest(t)()
	}
}

func setUpService(
	t testing.TB,
	rpcContext *rpc.Context,
	localNodeID roachpb.NodeID,
	remoteNodeID roachpb.NodeID,
	localExternalDir string,
	remoteExternalDir string,
) BlobClientFactory {
	s := rpc.NewServer(rpcContext)
	remoteBlobServer, err := NewBlobService(remoteExternalDir)
	if err != nil {
		t.Fatal(err)
	}
	blobspb.RegisterBlobServer(s, remoteBlobServer)
	ln, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	s2 := rpc.NewServer(rpcContext)
	localBlobServer, err := NewBlobService(localExternalDir)
	if err != nil {
		t.Fatal(err)
	}
	blobspb.RegisterBlobServer(s2, localBlobServer)
	ln2, err := netutil.ListenAndServeGRPC(rpcContext.Stopper, s2, util.TestAddr)
	if err != nil {
		t.Fatal(err)
	}

	localDialer := nodedialer.New(rpcContext,
		func(nodeID roachpb.NodeID) (net.Addr, error) {
			if nodeID == remoteNodeID {
				return ln.Addr(), nil
			} else if nodeID == localNodeID {
				return ln2.Addr(), nil
			}
			return nil, errors.Errorf("node %d not found", nodeID)
		},
	)
	return NewBlobClientFactory(
		localNodeID,
		localDialer,
		localExternalDir,
	)
}
