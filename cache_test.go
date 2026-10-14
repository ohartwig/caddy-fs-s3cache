// SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
// SPDX-License-Identifier: MIT

package s3cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

// fakeS3 is the smallest S3 that exercises the cache: GET with
// If-None-Match, HEAD bucket, 404 as NoSuchKey XML. It counts requests per
// method and key so tests can assert round trips.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]fakeObject
	calls   map[string]int // "GET key" / "HEAD /"
	fail    atomic.Bool    // when set, every request returns 500
	srv     *httptest.Server
}

type fakeObject struct {
	body     string
	etag     string
	modified time.Time
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{objects: map[string]fakeObject{}, calls: map[string]int{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeS3) put(key, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{
		body:     body,
		etag:     fmt.Sprintf("%q", fmt.Sprintf("%x", len(body))+"-"+strings.ToLower(key)+"-"+fmt.Sprint(len(f.calls))),
		modified: time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC).Add(time.Duration(len(body)) * time.Second),
	}
}

func (f *fakeS3) del(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
}

func (f *fakeS3) count(method, key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method+" "+key]
}

func (f *fakeS3) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, v := range f.calls {
		n += v
	}
	return n
}

func (f *fakeS3) handle(w http.ResponseWriter, r *http.Request) {
	if f.fail.Load() {
		http.Error(w, "boom", http.StatusInternalServerError)
		return
	}
	// path style: /bucket/key
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	f.mu.Lock()
	f.calls[r.Method+" "+key]++
	obj, ok := f.objects[key]
	f.mu.Unlock()

	if key == "" { // HeadBucket / ListObjects
		w.WriteHeader(http.StatusOK)
		return
	}
	if !ok {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
		return
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && inm == obj.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.modified.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", fmt.Sprint(len(obj.body)))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = io.WriteString(w, obj.body)
	}
}

func (f *fakeS3) client() *s3.Client {
	return s3.New(s3.Options{
		Region:       "eu-north-1",
		BaseEndpoint: aws.String(f.srv.URL),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("test", "secret", ""),
		HTTPClient:   f.srv.Client(),
		// A fake without checksums; the SDK would otherwise log per request.
		DisableLogOutputChecksumValidationSkipped: true,
	})
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestCache(t *testing.T, f *fakeS3, clk *clock, mutate func(*cacheOptions)) *cache {
	t.Helper()
	o := cacheOptions{
		dir:             t.TempDir(),
		maxSize:         1 << 20,
		revalidateAfter: 60 * time.Second,
		negativeTTL:     30 * time.Second,
		bucket:          "media",
		prefix:          "",
		client:          f.client(),
		logger:          zap.NewNop(),
		now:             clk.now,
	}
	if mutate != nil {
		mutate(&o)
	}
	c, err := newCache(o)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func readAll(t *testing.T, fsys fs.FS, name string) string {
	t.Helper()
	fh, err := fsys.Open(name)
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	defer fh.Close()
	b, err := io.ReadAll(fh)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(b)
}

type fsysAdapter struct{ c *cache }

func (a fsysAdapter) Open(name string) (fs.File, error)     { return a.c.open(name) }
func (a fsysAdapter) Stat(name string) (fs.FileInfo, error) { return a.c.stat(name) }

func TestStatAndOpenCostOneGet(t *testing.T) {
	f := newFakeS3(t)
	f.put("user_upload/a.jpg", "hello")
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, nil)
	fsys := fsysAdapter{c}

	// The sequence file_server performs: Stat, Open, Seek(end), Seek(start), Read.
	info, err := fsys.Stat("user_upload/a.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 5 || info.IsDir() || info.Name() != "a.jpg" {
		t.Fatalf("unexpected info %+v", info)
	}
	if !info.ModTime().Equal(time.Date(2026, 10, 14, 12, 0, 5, 0, time.UTC)) {
		t.Fatalf("modtime should follow Last-Modified, got %v", info.ModTime())
	}
	fh, err := fsys.Open("user_upload/a.jpg")
	if err != nil {
		t.Fatal(err)
	}
	seeker := fh.(io.Seeker)
	if n, err := seeker.Seek(0, io.SeekEnd); err != nil || n != 5 {
		t.Fatalf("seek end: %d %v", n, err)
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(fh)
	_ = fh.Close()
	if string(b) != "hello" {
		t.Fatalf("body %q", b)
	}
	if got := f.total(); got != 1 {
		t.Fatalf("expected exactly one S3 request for stat+open+seek+read, got %d", got)
	}

	// Second request within revalidate_after: no S3 at all.
	if readAll(t, fsys, "user_upload/a.jpg") != "hello" || f.total() != 1 {
		t.Fatalf("hot path must not touch S3, total=%d", f.total())
	}
}

func TestRevalidationUsesConditionalGet(t *testing.T) {
	f := newFakeS3(t)
	f.put("x.txt", "v1")
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, nil)
	fsys := fsysAdapter{c}

	readAll(t, fsys, "x.txt")
	clk.advance(61 * time.Second)
	if readAll(t, fsys, "x.txt") != "v1" {
		t.Fatal("content changed without a change upstream")
	}
	if f.count("GET", "x.txt") != 2 {
		t.Fatalf("expected a conditional GET after revalidate_after, got %d GETs", f.count("GET", "x.txt"))
	}
	// 304 refreshed the clock: the next minute is free again.
	clk.advance(30 * time.Second)
	readAll(t, fsys, "x.txt")
	if f.count("GET", "x.txt") != 2 {
		t.Fatalf("304 must reset the revalidation window, got %d GETs", f.count("GET", "x.txt"))
	}

	// Replace the object (same key, new ETag): picked up at the next revalidation.
	f.put("x.txt", "version-two")
	clk.advance(61 * time.Second)
	if got := readAll(t, fsys, "x.txt"); got != "version-two" {
		t.Fatalf("expected new content after revalidation, got %q", got)
	}
	info, _ := fsys.Stat("x.txt")
	if info.Size() != int64(len("version-two")) {
		t.Fatalf("stat must reflect the new size, got %d", info.Size())
	}
	if entries, size := c.stats(); entries != 1 || size != int64(len("version-two")) {
		t.Fatalf("index out of sync: entries=%d size=%d", entries, size)
	}
}

func TestMissIsNegativelyCached(t *testing.T) {
	f := newFakeS3(t)
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, nil)
	fsys := fsysAdapter{c}

	for i := 0; i < 5; i++ {
		_, err := fsys.Stat("nope.jpg")
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("expected ErrNotExist, got %v", err)
		}
		_, err = fsys.Open("nope.jpg")
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("expected ErrNotExist on open, got %v", err)
		}
	}
	if f.count("GET", "nope.jpg") != 1 {
		t.Fatalf("a miss must cost one GET within negative_ttl, got %d", f.count("GET", "nope.jpg"))
	}

	// The object appears: visible once the negative entry expires.
	f.put("nope.jpg", "now here")
	clk.advance(31 * time.Second)
	if readAll(t, fsys, "nope.jpg") != "now here" {
		t.Fatal("object should appear after negative_ttl")
	}

	// And disappears again: a 404 on revalidation evicts the body.
	f.del("nope.jpg")
	clk.advance(61 * time.Second)
	if _, err := fsys.Stat("nope.jpg"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("deleted object must vanish after revalidation, got %v", err)
	}
	if entries, size := c.stats(); entries != 1 || size != 0 {
		t.Fatalf("negative entry should hold no bytes: entries=%d size=%d", entries, size)
	}
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	f := newFakeS3(t)
	for i := 0; i < 4; i++ {
		f.put(fmt.Sprintf("f%d", i), strings.Repeat("x", 100))
	}
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, func(o *cacheOptions) { o.maxSize = 250 })
	fsys := fsysAdapter{c}

	readAll(t, fsys, "f0")
	readAll(t, fsys, "f1")
	readAll(t, fsys, "f0") // f0 is now more recent than f1
	readAll(t, fsys, "f2") // 300 > 250: evicts f1
	if entries, size := c.stats(); entries != 2 || size != 200 {
		t.Fatalf("expected two entries / 200 bytes, got %d / %d", entries, size)
	}
	readAll(t, fsys, "f0")
	if f.count("GET", "f0") != 1 {
		t.Fatalf("f0 was recently used and must survive eviction, GETs=%d", f.count("GET", "f0"))
	}
	readAll(t, fsys, "f1")
	if f.count("GET", "f1") != 2 {
		t.Fatalf("f1 was evicted and must be fetched again, GETs=%d", f.count("GET", "f1"))
	}
}

func TestConcurrentRequestsShareOneFetch(t *testing.T) {
	f := newFakeS3(t)
	f.put("big.bin", strings.Repeat("b", 64<<10))
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, nil)
	fsys := fsysAdapter{c}

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if len(readAll(t, fsys, "big.bin")) != 64<<10 {
				t.Error("short read")
			}
		}()
	}
	wg.Wait()
	if got := f.count("GET", "big.bin"); got != 1 {
		t.Fatalf("expected one shared GET for 32 concurrent readers, got %d", got)
	}
}

func TestServesStaleWhenS3Fails(t *testing.T) {
	f := newFakeS3(t)
	f.put("a.txt", "cached")
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, nil)
	fsys := fsysAdapter{c}

	readAll(t, fsys, "a.txt")
	f.fail.Store(true)
	clk.advance(61 * time.Second)
	if got := readAll(t, fsys, "a.txt"); got != "cached" {
		t.Fatalf("expected the stale copy while S3 is down, got %q", got)
	}
	// An object we never had cannot be served, and the failure must not be
	// dressed as a *fs.PathError: Caddy's file_server answers 400 for those,
	// 500/503 for a bare error.
	_, err := fsys.Stat("never.txt")
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("an upstream failure must not look like a 404, got %v", err)
	}
	var pe *fs.PathError
	if errors.As(err, &pe) {
		t.Fatalf("upstream failure must be a bare error, got PathError %v", err)
	}
	// Whereas a miss and an invalid name are PathErrors, as fs.FS promises.
	f.fail.Store(false)
	if _, err := fsys.Stat("never.txt"); !errors.As(err, &pe) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("miss must be a PathError wrapping ErrNotExist, got %v", err)
	}
	if _, err := fsys.Open("../x"); !errors.As(err, &pe) || !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("invalid name must be a PathError wrapping ErrInvalid, got %v", err)
	}
}

func TestInvalidNamesAreRejectedWithoutS3(t *testing.T) {
	f := newFakeS3(t)
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, nil)
	fsys := fsysAdapter{c}
	for _, name := range []string{"../etc/passwd", "/abs", "a//b", "dir/", "a/./b"} {
		_, err := fsys.Stat(name)
		if !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("%q: expected ErrInvalid, got %v", name, err)
		}
	}
	if f.total() != 0 {
		t.Fatalf("invalid names must not reach S3, got %d requests", f.total())
	}
	// The root opens as a directory, without S3.
	info, err := fsys.Stat(".")
	if err != nil || !info.IsDir() {
		t.Fatalf("root must be a directory: %v %v", info, err)
	}
	if f.total() != 0 {
		t.Fatalf("root stat must not reach S3")
	}
}

func TestPrefixIsPrependedToKeys(t *testing.T) {
	f := newFakeS3(t)
	f.put("media/img.png", "png")
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, func(o *cacheOptions) { o.prefix = "media/" })
	if readAll(t, fsysAdapter{c}, "img.png") != "png" {
		t.Fatal("prefix not applied")
	}
	if f.count("GET", "media/img.png") != 1 {
		t.Fatal("expected the prefixed key to be requested")
	}
}

func TestCacheDirIsEmptiedOnStart(t *testing.T) {
	f := newFakeS3(t)
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	dir := t.TempDir()
	stale := dir + "/leftover"
	if err := os.WriteFile(stale, []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	newTestCache(t, f, clk, func(o *cacheOptions) { o.dir = dir })
	if _, err := os.Stat(stale); err == nil {
		t.Fatal("leftover file must be removed when the cache starts")
	}
}

func TestNotFoundDetection(t *testing.T) {
	// The SDK wraps the fake's 404 into a NoSuchKey error; make sure both
	// detection paths agree, because a wrong classification would turn a
	// 404 into a 500 at the edge.
	f := newFakeS3(t)
	_, err := f.client().GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("media"), Key: aws.String("missing"),
	})
	if !isNotFound(err) {
		t.Fatalf("404 not recognised: %v", err)
	}
	if isNotModified(err) {
		t.Fatal("404 misread as 304")
	}
}
