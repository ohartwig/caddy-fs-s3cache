// SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
// SPDX-License-Identifier: MIT

package s3cache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// objectGetter is the slice of the S3 client the cache needs. It exists so
// tests can count calls without a network.
type objectGetter interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type cacheOptions struct {
	dir             string
	maxSize         int64
	revalidateAfter time.Duration
	negativeTTL     time.Duration
	bucket          string
	prefix          string
	client          objectGetter
	logger          *zap.Logger
	now             func() time.Time // tests override
}

// entry is one cached key. A positive entry has a body file on disk; a
// negative entry remembers a 404.
type entry struct {
	key       string
	path      string // body file, empty for negative entries
	etag      string
	size      int64
	modTime   time.Time
	fetchedAt time.Time // last time S3 confirmed this state (200 or 304)
	negative  bool
	elem      *list.Element // position in the LRU list
}

type cache struct {
	opts cacheOptions
	now  func() time.Time

	mu      sync.Mutex
	entries map[string]*entry
	lru     *list.List // front = most recently used
	size    int64      // sum of positive entry sizes

	group singleflight.Group
}

func newCache(o cacheOptions) (*cache, error) {
	if o.now == nil {
		o.now = time.Now
	}
	if o.logger == nil {
		o.logger = zap.NewNop()
	}
	if err := os.MkdirAll(o.dir, 0o700); err != nil {
		return nil, fmt.Errorf("create cache dir %s: %w", o.dir, err)
	}
	// The index starts empty, so whatever is on disk is unreachable; drop it
	// rather than leak it. The directory is private to this module.
	dents, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, fmt.Errorf("read cache dir %s: %w", o.dir, err)
	}
	for _, d := range dents {
		_ = os.RemoveAll(filepath.Join(o.dir, d.Name()))
	}
	return &cache{
		opts:    o,
		now:     o.now,
		entries: make(map[string]*entry),
		lru:     list.New(),
	}, nil
}

// dirInfo is the synthetic FileInfo for the root. file_server only ever
// asks for it when the request path is empty, which our handle never
// produces, but fs.FS demands that "." opens.
type dirInfo struct{ name string }

func (d dirInfo) Name() string       { return d.name }
func (d dirInfo) Size() int64        { return 0 }
func (d dirInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o555 }
func (d dirInfo) ModTime() time.Time { return time.Time{} }
func (d dirInfo) IsDir() bool        { return true }
func (d dirInfo) Sys() any           { return nil }

type dirFile struct{ info dirInfo }

func (d *dirFile) Stat() (fs.FileInfo, error) { return d.info, nil }
func (d *dirFile) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.name, Err: errors.New("is a directory")}
}
func (d *dirFile) Close() error                       { return nil }
func (d *dirFile) ReadDir(int) ([]fs.DirEntry, error) { return nil, nil }

// fileInfo is what Stat returns for a cached object. ModTime is the
// object's Last-Modified, so Caddy's ETag (mtime + size) and Last-Modified
// header follow the object, not the cache file.
type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi fileInfo) Name() string       { return path.Base(fi.name) }
func (fi fileInfo) Size() int64        { return fi.size }
func (fi fileInfo) Mode() fs.FileMode  { return 0o444 }
func (fi fileInfo) ModTime() time.Time { return fi.modTime }
func (fi fileInfo) IsDir() bool        { return false }
func (fi fileInfo) Sys() any           { return nil }

// file wraps the cache body on disk. Seek and ReadAt are local, which is the
// whole point: http.ServeContent can seek to the end and back for free.
type file struct {
	*os.File
	info fileInfo
}

func (f *file) Stat() (fs.FileInfo, error) { return f.info, nil }

func (c *cache) stat(name string) (fs.FileInfo, error) {
	if name == "." {
		return dirInfo{name: "."}, nil
	}
	e, err := c.lookup(name)
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	return fileInfo{name: name, size: e.size, modTime: e.modTime}, nil
}

func (c *cache) open(name string) (fs.File, error) {
	if name == "." {
		return &dirFile{info: dirInfo{name: "."}}, nil
	}
	e, err := c.lookup(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	fh, err := os.Open(e.path)
	if err != nil {
		// Evicted between lookup and open: a narrow race. Fetch again once.
		c.forget(name)
		e, err = c.lookup(name)
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
		fh, err = os.Open(e.path)
		if err != nil {
			return nil, &fs.PathError{Op: "open", Path: name, Err: err}
		}
	}
	return &file{File: fh, info: fileInfo{name: name, size: e.size, modTime: e.modTime}}, nil
}

// lookup returns a fresh positive entry for name, fetching or revalidating
// as needed. Concurrent callers for the same key share one S3 request.
func (c *cache) lookup(name string) (*entry, error) {
	if !fs.ValidPath(name) || strings.HasSuffix(name, "/") {
		return nil, fs.ErrInvalid
	}
	now := c.now()

	c.mu.Lock()
	e, ok := c.entries[name]
	if ok {
		c.lru.MoveToFront(e.elem)
		if e.negative {
			if now.Sub(e.fetchedAt) < c.opts.negativeTTL {
				c.mu.Unlock()
				return nil, fs.ErrNotExist
			}
		} else if now.Sub(e.fetchedAt) < c.opts.revalidateAfter {
			c.mu.Unlock()
			return e, nil
		}
	}
	c.mu.Unlock()

	v, err, _ := c.group.Do(name, func() (any, error) {
		return c.fetch(name)
	})
	if err != nil {
		return nil, err
	}
	return v.(*entry), nil
}

// fetch performs one (conditional) GET and updates the index.
func (c *cache) fetch(name string) (*entry, error) {
	c.mu.Lock()
	prev := c.entries[name]
	var etag string
	if prev != nil && !prev.negative {
		etag = prev.etag
	}
	c.mu.Unlock()

	in := &s3.GetObjectInput{
		Bucket: aws.String(c.opts.bucket),
		Key:    aws.String(c.opts.prefix + name),
	}
	if etag != "" {
		in.IfNoneMatch = aws.String(etag)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := c.opts.client.GetObject(ctx, in)
	now := c.now()

	switch {
	case err == nil:
		defer out.Body.Close()
		return c.store(name, out, now)

	case isNotModified(err):
		c.mu.Lock()
		defer c.mu.Unlock()
		if prev == nil || prev.negative {
			// Cannot happen without a prior positive entry; treat as miss.
			return nil, fs.ErrNotExist
		}
		prev.fetchedAt = now
		return prev, nil

	case isNotFound(err):
		c.mu.Lock()
		defer c.mu.Unlock()
		c.removeLocked(name)
		e := &entry{key: name, negative: true, fetchedAt: now}
		e.elem = c.lru.PushFront(e)
		c.entries[name] = e
		return nil, fs.ErrNotExist

	default:
		// S3 unreachable or denied. Serve a stale copy if there is one; the
		// alternative is an error page for an object we actually have.
		c.mu.Lock()
		defer c.mu.Unlock()
		if prev != nil && !prev.negative {
			c.opts.logger.Warn("s3cache: revalidation failed, serving stale",
				zap.String("key", name), zap.Error(err))
			return prev, nil
		}
		c.opts.logger.Error("s3cache: fetch failed", zap.String("key", name), zap.Error(err))
		return nil, fmt.Errorf("s3cache: get %s: %w", name, err)
	}
}

// store streams a 200 response into the cache directory and indexes it.
func (c *cache) store(name string, out *s3.GetObjectOutput, now time.Time) (*entry, error) {
	tmp, err := os.CreateTemp(c.opts.dir, "incoming-*")
	if err != nil {
		return nil, fmt.Errorf("s3cache: temp file: %w", err)
	}
	tmpName := tmp.Name()
	n, err := io.Copy(tmp, out.Body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmpName)
		return nil, fmt.Errorf("s3cache: download %s: %w", name, err)
	}
	if out.ContentLength != nil && *out.ContentLength != n {
		_ = os.Remove(tmpName)
		return nil, fmt.Errorf("s3cache: download %s: short body (%d of %d bytes)", name, n, *out.ContentLength)
	}

	final := filepath.Join(c.opts.dir, keyFilename(name))
	if err := os.Rename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return nil, fmt.Errorf("s3cache: place %s: %w", name, err)
	}

	e := &entry{
		key:       name,
		path:      final,
		etag:      aws.ToString(out.ETag),
		size:      n,
		fetchedAt: now,
	}
	if out.LastModified != nil {
		e.modTime = *out.LastModified
	} else {
		e.modTime = now
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if prev := c.entries[name]; prev != nil {
		// Same final path: the rename already replaced the body on disk.
		c.lru.Remove(prev.elem)
		if !prev.negative {
			c.size -= prev.size
		}
		delete(c.entries, name)
	}
	e.elem = c.lru.PushFront(e)
	c.entries[name] = e
	c.size += n
	c.evictLocked()
	return e, nil
}

// evictLocked drops least recently used entries until the cache fits. The
// entry just inserted sits at the front and is never evicted here, even if
// it alone exceeds the limit; the next insert will take it.
func (c *cache) evictLocked() {
	for c.size > c.opts.maxSize && c.lru.Len() > 1 {
		back := c.lru.Back()
		e := back.Value.(*entry)
		c.removeLocked(e.key)
	}
}

func (c *cache) removeLocked(name string) {
	e, ok := c.entries[name]
	if !ok {
		return
	}
	c.lru.Remove(e.elem)
	delete(c.entries, name)
	if !e.negative {
		c.size -= e.size
		// Open handles keep reading the unlinked inode.
		_ = os.Remove(e.path)
	}
}

func (c *cache) forget(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(name)
}

// stats is a snapshot for tests and logs.
func (c *cache) stats() (entries int, size int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries), c.size
}

func keyFilename(name string) string {
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:]) + filepath.Ext(name)
}

func isNotModified(err error) bool {
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == 304
}

func isNotFound(err error) bool {
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 404 {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}
