// SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
// SPDX-License-Identifier: MIT

package s3cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"
)

// missGetter answers every GetObject with NoSuchKey, without a network, so a
// test can push a hundred thousand distinct misses through the cache quickly.
type missGetter struct{ calls atomic.Int64 }

func (m *missGetter) GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.calls.Add(1)
	return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "The specified key does not exist."}
}

func newMissCache(t *testing.T, clk *clock, g objectGetter, mutate func(*cacheOptions)) *cache {
	t.Helper()
	o := cacheOptions{
		dir:             t.TempDir(),
		maxSize:         1 << 20,
		revalidateAfter: 60 * time.Second,
		negativeTTL:     30 * time.Second,
		maxNegative:     DefaultNegativeMaxEntries,
		bucket:          "media",
		client:          g,
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

func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// AUD-023-F01 reproduction R1: distinct misses from the internet must not
// grow the index without bound. Before the fix the index held one negative
// entry per distinct missing key (100 000 entries, ~66 MiB of heap).
func TestDistinctMissesAreBounded(t *testing.T) {
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	g := &missGetter{}
	c := newMissCache(t, clk, g, nil)
	fsys := fsysAdapter{c}

	const n = 100_000
	pad := strings.Repeat("x", 200)
	before := heapAlloc()
	for i := 0; i < n; i++ {
		if _, err := fsys.Stat(fmt.Sprintf("missing/%d/%s.png", i, pad)); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("miss %d: expected ErrNotExist, got %v", i, err)
		}
	}
	after := heapAlloc()

	entries, size := c.stats()
	t.Logf("entries=%d size=%d s3 calls=%d heap delta=%d KiB", entries, size, g.calls.Load(), (int64(after)-int64(before))/1024)
	if entries > DefaultNegativeMaxEntries {
		t.Fatalf("negative entries must stay at or below the cap %d, got %d", DefaultNegativeMaxEntries, entries)
	}
	// The cap is a few thousand entries of ~650 bytes; the unbounded index
	// grew by ~66 MiB. 8 MiB leaves room for GC noise and nothing else.
	if delta := int64(after) - int64(before); delta > 8<<20 {
		t.Fatalf("heap must stay flat across %d misses, grew by %d KiB", n, delta/1024)
	}
	if g.calls.Load() != n {
		t.Fatalf("every distinct miss costs one GET, got %d", g.calls.Load())
	}
}

// AUD-023-F01 proof of fix, second half: a negative entry older than
// negative_ttl is gone after the sweep, without a request for its key.
func TestExpiredNegativeIsSwept(t *testing.T) {
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newMissCache(t, clk, &missGetter{}, nil)
	fsys := fsysAdapter{c}

	for i := 0; i < 10; i++ {
		_, _ = fsys.Stat(fmt.Sprintf("gone/%d.jpg", i))
	}
	if entries, _ := c.stats(); entries != 10 {
		t.Fatalf("expected 10 negative entries, got %d", entries)
	}

	// Idle process: the janitor's sweep alone removes the expired misses.
	clk.advance(31 * time.Second)
	c.sweepNegatives()
	if entries, _ := c.stats(); entries != 0 {
		t.Fatalf("expired negative entries must be swept without a request for their key, %d left", entries)
	}

	// Busy process: an unrelated miss sweeps the expired ones on insert.
	for i := 0; i < 10; i++ {
		_, _ = fsys.Stat(fmt.Sprintf("old/%d.jpg", i))
	}
	clk.advance(31 * time.Second)
	_, _ = fsys.Stat("unrelated.jpg")
	if entries, _ := c.stats(); entries != 1 {
		t.Fatalf("an insert must sweep expired negatives, %d entries left (want 1)", entries)
	}
}

// The negative cap evicts the oldest miss first and leaves positive entries
// alone.
func TestNegativeCapEvictsOldestMissOnly(t *testing.T) {
	f := newFakeS3(t)
	f.put("hot.jpg", "body")
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	c := newTestCache(t, f, clk, func(o *cacheOptions) { o.maxNegative = 3 })
	fsys := fsysAdapter{c}

	if readAll(t, fsys, "hot.jpg") != "body" {
		t.Fatal("hot.jpg")
	}
	for _, k := range []string{"m1.jpg", "m2.jpg", "m3.jpg", "m4.jpg"} {
		_, _ = fsys.Stat(k)
		clk.advance(time.Second)
	}
	if entries, size := c.stats(); entries != 4 || size != int64(len("body")) {
		t.Fatalf("want 1 positive + 3 negative entries, got entries=%d size=%d", entries, size)
	}
	// m1 was evicted: asking again costs a GET; m4 is still remembered.
	_, _ = fsys.Stat("m1.jpg")
	_, _ = fsys.Stat("m4.jpg")
	if f.count("GET", "m1.jpg") != 2 || f.count("GET", "m4.jpg") != 1 {
		t.Fatalf("oldest miss must be evicted first: m1=%d m4=%d", f.count("GET", "m1.jpg"), f.count("GET", "m4.jpg"))
	}
	if f.count("GET", "hot.jpg") != 1 {
		t.Fatal("positive entry must survive the negative cap")
	}
}

// Keys longer than S3's own limit are refused before S3 is asked.
func TestOverlongKeyIsRefusedWithoutS3(t *testing.T) {
	clk := &clock{t: time.Unix(1_800_000_000, 0)}
	g := &missGetter{}
	c := newMissCache(t, clk, g, func(o *cacheOptions) { o.prefix = "media/" })
	fsys := fsysAdapter{c}

	long := strings.Repeat("a", maxKeyLength-len("media/")+1)
	if _, err := fsys.Stat(long); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("overlong key must be fs.ErrInvalid, got %v", err)
	}
	if g.calls.Load() != 0 {
		t.Fatalf("overlong key must not reach S3, got %d calls", g.calls.Load())
	}
	if entries, _ := c.stats(); entries != 0 {
		t.Fatalf("overlong key must not be indexed, got %d", entries)
	}
	if _, err := fsys.Stat(long[1:]); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a key of exactly %d bytes is valid, got %v", maxKeyLength, err)
	}
}
