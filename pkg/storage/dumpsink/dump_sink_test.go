package dumpsink

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/znbasedb/znbase/pkg/blobs"
	"github.com/znbasedb/znbase/pkg/settings/cluster"
	"github.com/znbasedb/znbase/pkg/testutils"
	"github.com/znbasedb/znbase/pkg/util/ctxgroup"
	"github.com/znbasedb/znbase/pkg/util/leaktest"
	"github.com/znbasedb/znbase/pkg/util/retry"
)

func init() {
	testSettings = cluster.MakeTestingClusterSettings()
}

func appendPath(t *testing.T, s, add string) string {
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = filepath.Join(u.Path, add)
	return u.String()
}

var testSettings *cluster.Settings

func storeFromURI(
	ctx context.Context, t *testing.T, uri string, clientFactory blobs.BlobClientFactory,
) DumpSink {
	conf, err := ConfFromURI(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	// Setup a sink for the given args.
	s, err := MakeDumpSink(ctx, conf, testSettings, clientFactory)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testExportStore(t *testing.T, storeURI string, skipSingleFile bool) {
	ctx := context.TODO()

	conf, err := ConfFromURI(ctx, storeURI)
	if err != nil {
		t.Fatal(err)
	}

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := MakeDumpSink(ctx, conf, testSettings, clientFactory)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if readConf := s.Conf(); readConf != conf {
		t.Fatalf("conf does not roundtrip: started with %+v, got back %+v", conf, readConf)
	}

	t.Run("simple round trip", func(t *testing.T) {
		sampleName := "somebytes"
		sampleBytes := "hello world"

		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("%s-%d", sampleName, i)
			payload := []byte(strings.Repeat(sampleBytes, i))
			if err := s.WriteFile(ctx, name, bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}

			r, err := s.ReadFile(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()

			res, err := ioutil.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(res, payload) {
				t.Fatalf("got %v expected %v", res, payload)
			}
			if err := s.Delete(ctx, name); err != nil {
				t.Fatal(err)
			}
		}
	})

	// The azure driver makes us chunk files that are greater than 4mb, so make
	// sure that files larger than that work on all the providers.
	t.Run("8mb-tempfile", func(t *testing.T) {
		const size = 1024 * 1024 * 8 // 8MiB
		testingContent := make([]byte, size)
		if _, err := rand.Read(testingContent); err != nil {
			t.Fatal(err)
		}
		testingFilename := "testing-123"

		// Write some random data (random so it doesn't compress).
		if err := s.WriteFile(ctx, testingFilename, bytes.NewReader(testingContent)); err != nil {
			t.Fatal(err)
		}

		// Attempt to read (or fetch) it back.
		res, err := s.ReadFile(ctx, testingFilename)
		if err != nil {
			t.Fatalf("Could not get reader for %s: %+v", testingFilename, err)
		}
		defer res.Close()
		content, err := ioutil.ReadAll(res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, testingContent) {
			t.Fatalf("wrong content")
		}
		if err := s.Delete(ctx, testingFilename); err != nil {
			t.Fatal(err)
		}
	})
	if skipSingleFile {
		return
	}
	t.Run("read-single-file-by-uri", func(t *testing.T) {
		const testingFilename = "A"
		if err := s.WriteFile(ctx, testingFilename, bytes.NewReader([]byte("aaa"))); err != nil {
			t.Fatal(err)
		}
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename), clientFactory)
		defer singleFile.Close()

		res, err := singleFile.ReadFile(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close()
		content, err := ioutil.ReadAll(res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, []byte("aaa")) {
			t.Fatalf("wrong content")
		}
		if err := s.Delete(ctx, testingFilename); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("write-single-file-by-uri", func(t *testing.T) {
		const testingFilename = "B"
		singleFile := storeFromURI(ctx, t, appendPath(t, storeURI, testingFilename), clientFactory)
		defer singleFile.Close()

		if err := singleFile.WriteFile(ctx, "", bytes.NewReader([]byte("bbb"))); err != nil {
			t.Fatal(err)
		}

		res, err := s.ReadFile(ctx, testingFilename)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close()
		content, err := ioutil.ReadAll(res)
		if err != nil {
			t.Fatal(err)
		}
		// Verify the result contains what we wrote.
		if !bytes.Equal(content, []byte("bbb")) {
			t.Fatalf("wrong content")
		}
		if err := s.Delete(ctx, testingFilename); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	testSettings.ExternalIODir = p
	dest := MakeLocalSinkURI(p)

	testExportStore(t, dest, false)
}

func TestPutHttp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tmp, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	makeServer := func() (*url.URL, func() int, func()) {
		var files int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			localfile := filepath.Join(tmp, filepath.Base(r.URL.Path))
			switch r.Method {
			case "PUT":
				f, err := os.Create(localfile)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				defer f.Close()
				defer r.Body.Close()
				if _, err := io.Copy(f, r.Body); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				files++
			case "GET":
				http.ServeFile(w, r, localfile)
			case "DELETE":
				if err := os.Remove(localfile); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
			default:
				http.Error(w, "unsupported method "+r.Method, 400)
			}
		}))
		t.Logf("Mock HTTP Storage %q", srv.URL)
		uri, err := url.Parse(srv.URL)
		if err != nil {
			srv.Close()
			t.Fatal(err)
		}
		uri.Path = filepath.Join(uri.Path, "testing")
		return uri, func() int { return files }, srv.Close
	}

	t.Run("singleHost", func(t *testing.T) {
		srv, files, cleanup := makeServer()
		defer cleanup()
		testExportStore(t, srv.String(), false)
		if expected, actual := 13, files(); expected != actual {
			t.Fatalf("expected %d files to be written to single http store, got %d", expected, actual)
		}
	})

	t.Run("multiHost", func(t *testing.T) {
		srv1, files1, cleanup1 := makeServer()
		defer cleanup1()
		srv2, files2, cleanup2 := makeServer()
		defer cleanup2()
		srv3, files3, cleanup3 := makeServer()
		defer cleanup3()

		combined := *srv1
		combined.Host = strings.Join([]string{srv1.Host, srv2.Host, srv3.Host}, ",")

		testExportStore(t, combined.String(), true)
		if expected, actual := 3, files1(); expected != actual {
			t.Fatalf("expected %d files written to http host 1, got %d", expected, actual)
		}
		if expected, actual := 4, files2(); expected != actual {
			t.Fatalf("expected %d files written to http host 2, got %d", expected, actual)
		}
		if expected, actual := 4, files3(); expected != actual {
			t.Fatalf("expected %d files written to http host 3, got %d", expected, actual)
		}
	})
}

func rangeStart(r string) (int, error) {
	if len(r) == 0 {
		return 0, nil
	}
	r = strings.TrimPrefix(r, "bytes=")

	return strconv.Atoi(r[:strings.IndexByte(r, '-')])
}

func TestHttpGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	data := []byte("to serve, or not to serve.  c'est la question")

	defer func(opts retry.Options) {
		HTTPRetryOptions = opts
	}(HTTPRetryOptions)

	HTTPRetryOptions.InitialBackoff = 1 * time.Microsecond
	HTTPRetryOptions.MaxBackoff = 10 * time.Millisecond
	HTTPRetryOptions.MaxRetries = 100

	for _, tc := range []int{1, 2, 5, 16, 32, len(data) - 1, len(data)} {
		t.Run(fmt.Sprintf("read-%d", tc), func(t *testing.T) {
			limit := tc
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				start, err := rangeStart(r.Header.Get("Range"))
				if start < 0 || start >= len(data) {
					t.Errorf("invalid start offset %d in range header %s",
						start, r.Header.Get("Range"))
				}
				end := start + limit
				if end > len(data) {
					end = len(data)
				}

				w.Header().Add("Accept-Ranges", "bytes")
				w.Header().Add("Content-Length", strconv.Itoa(len(data)-start))

				if start > 0 {
					w.Header().Add(
						"Content-Range",
						fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
				}

				if err == nil {
					_, err = w.Write(data[start:end])
				}
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
				}
			}))

			// Start antagonist function that aggressively closes client connections.
			ctx, cancelAntagonist := context.WithCancel(context.Background())
			g := ctxgroup.WithContext(ctx)
			g.GoCtx(func(ctx context.Context) error {
				opts := retry.Options{
					InitialBackoff: 500 * time.Microsecond,
					MaxBackoff:     1 * time.Millisecond,
				}
				for attempt := retry.StartWithCtx(ctx, opts); attempt.Next(); {
					s.CloseClientConnections()
				}
				return nil
			})

			store, err := makeHTTPSink(s.URL, testSettings, "")
			require.NoError(t, err)

			var file io.ReadCloser

			// Cleanup.
			defer func() {
				s.Close()
				if store != nil {
					require.NoError(t, store.Close())
				}
				if file != nil {
					require.NoError(t, file.Close())
				}
				cancelAntagonist()
				_ = g.Wait()
			}()

			// Read the file and verify results.
			file, err = store.ReadFile(ctx, "/something")
			require.NoError(t, err)

			b, err := ioutil.ReadAll(file)
			require.NoError(t, err)
			require.EqualValues(t, data, b)
		})
	}
}
