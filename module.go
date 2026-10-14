// SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
// SPDX-License-Identifier: MIT

// Package s3cache implements a Caddy virtual filesystem (`caddy.fs.s3cache`)
// that serves objects from an S3 bucket through a local disk cache.
//
// It exists because `file_server` on a plain S3 filesystem costs three
// round trips per file (HEAD for Stat, GET for Open, and a second GET when
// http.ServeContent seeks back to the start) and nothing is remembered
// between requests. This module fetches an object once with a single GET,
// keeps it on local disk, answers Stat and Open from that copy, and
// revalidates with If-None-Match after a configurable interval. A hot object
// is served without any S3 request; a cold object costs one.
package s3cache

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(FS{})
}

// Interface guards.
var (
	_ fs.StatFS             = (*FS)(nil)
	_ caddy.Provisioner     = (*FS)(nil)
	_ caddy.CleanerUpper    = (*FS)(nil)
	_ caddyfile.Unmarshaler = (*FS)(nil)
)

// Defaults applied by Provision when a field is zero.
const (
	DefaultCacheMaxSize    int64 = 512 << 20 // 512 MiB
	DefaultRevalidateAfter       = 60 * time.Second
	DefaultNegativeTTL           = 30 * time.Second
	DefaultKeepalive             = 25 * time.Second
	DefaultMaxIdleConns          = 16
)

// FS is a Caddy virtual filesystem backed by an S3 bucket with a local disk cache.
type FS struct {
	// Bucket is the S3 bucket name (required).
	Bucket string `json:"bucket,omitempty"`

	// Region is the AWS region of the bucket. Empty falls back to the SDK's
	// default resolution (AWS_REGION and friends).
	Region string `json:"region,omitempty"`

	// Endpoint overrides the S3 endpoint, for S3-compatible stores.
	Endpoint string `json:"endpoint,omitempty"`

	// UsePathStyle switches to path-style addressing (needed by most
	// S3-compatible test servers).
	UsePathStyle bool `json:"use_path_style,omitempty"`

	// Prefix is prepended to every key. A filesystem path `a/b.jpg` with
	// prefix `media/` reads the object `media/a/b.jpg`.
	Prefix string `json:"prefix,omitempty"`

	// CacheDir holds the cached object bodies. Default: a directory below
	// os.TempDir(). The directory is emptied when the module is provisioned,
	// because the in-memory index starts empty and would otherwise leak files.
	CacheDir string `json:"cache_dir,omitempty"`

	// CacheMaxSize bounds the cache in bytes; least recently used objects are
	// evicted first. Default 512 MiB.
	CacheMaxSize int64 `json:"cache_max_size,omitempty"`

	// RevalidateAfter is how long a cached object is served without asking S3
	// whether it changed. After that, the next access sends a conditional GET
	// (If-None-Match); a 304 costs one round trip and no body. Default 60s.
	RevalidateAfter caddy.Duration `json:"revalidate_after,omitempty"`

	// NegativeTTL is how long a missing key stays missing without asking S3
	// again. Default 30s.
	NegativeTTL caddy.Duration `json:"negative_ttl,omitempty"`

	// Keepalive is the interval of a background HeadBucket request that keeps
	// a TLS connection to S3 open, so the first request after a quiet period
	// does not pay for a new handshake. 0 disables it. Default 25s.
	Keepalive caddy.Duration `json:"keepalive,omitempty"`

	// MaxIdleConns bounds the pooled connections to S3. Default 16.
	MaxIdleConns int `json:"max_idle_conns,omitempty"`

	cache  *cache
	cancel context.CancelFunc
	logger *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (FS) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "caddy.fs.s3cache",
		New: func() caddy.Module { return new(FS) },
	}
}

// Provision implements caddy.Provisioner.
func (f *FS) Provision(ctx caddy.Context) error {
	f.logger = ctx.Logger()

	if f.Bucket == "" {
		return errors.New("bucket must be set")
	}
	if f.CacheMaxSize <= 0 {
		f.CacheMaxSize = DefaultCacheMaxSize
	}
	if f.RevalidateAfter == 0 {
		f.RevalidateAfter = caddy.Duration(DefaultRevalidateAfter)
	}
	if f.NegativeTTL == 0 {
		f.NegativeTTL = caddy.Duration(DefaultNegativeTTL)
	}
	switch {
	case f.Keepalive == 0:
		f.Keepalive = caddy.Duration(DefaultKeepalive)
	case f.Keepalive < 0: // "keepalive off" in the Caddyfile
		f.Keepalive = 0
	}
	if f.MaxIdleConns <= 0 {
		f.MaxIdleConns = DefaultMaxIdleConns
	}
	if f.CacheDir == "" {
		f.CacheDir = filepath.Join(os.TempDir(), "caddy-fs-s3cache", f.Bucket)
	}
	if f.Prefix != "" && !strings.HasSuffix(f.Prefix, "/") {
		f.Prefix += "/"
	}

	client, err := f.newClient(ctx)
	if err != nil {
		return err
	}

	c, err := newCache(cacheOptions{
		dir:             f.CacheDir,
		maxSize:         f.CacheMaxSize,
		revalidateAfter: time.Duration(f.RevalidateAfter),
		negativeTTL:     time.Duration(f.NegativeTTL),
		bucket:          f.Bucket,
		prefix:          f.Prefix,
		client:          client,
		logger:          f.logger,
	})
	if err != nil {
		return fmt.Errorf("s3cache: %w", err)
	}
	f.cache = c

	if f.Keepalive > 0 {
		var kctx context.Context
		kctx, f.cancel = context.WithCancel(context.Background())
		go f.keepalive(kctx, client, time.Duration(f.Keepalive))
	}

	f.logger.Info("s3cache provisioned",
		zap.String("bucket", f.Bucket),
		zap.String("prefix", f.Prefix),
		zap.String("cache_dir", f.CacheDir),
		zap.Int64("cache_max_size", f.CacheMaxSize),
		zap.Duration("revalidate_after", time.Duration(f.RevalidateAfter)),
		zap.Duration("negative_ttl", time.Duration(f.NegativeTTL)),
		zap.Duration("keepalive", time.Duration(f.Keepalive)),
	)
	return nil
}

// Cleanup implements caddy.CleanerUpper.
func (f *FS) Cleanup() error {
	if f.cancel != nil {
		f.cancel()
	}
	return nil
}

func (f *FS) newClient(ctx caddy.Context) (*s3.Client, error) {
	var opts []func(*config.LoadOptions) error
	if f.Region != "" {
		opts = append(opts, config.WithRegion(f.Region))
	}
	// One transport per module instance: pooled, long idle timeout, so the
	// keepalive request can actually keep a connection warm.
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          f.MaxIdleConns,
		MaxIdleConnsPerHost:   f.MaxIdleConns,
		IdleConnTimeout:       10 * time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
	opts = append(opts, config.WithHTTPClient(&http.Client{Transport: transport}))

	cfg, err := config.LoadDefaultConfig(ctx.Context, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3cache: load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if f.Endpoint != "" {
			o.BaseEndpoint = aws.String(f.Endpoint)
		}
		o.UsePathStyle = f.UsePathStyle
		// Response checksums are not validated for GetObject without a
		// checksum mode; silence the SDK's per-request warning about it.
		o.DisableLogOutputChecksumValidationSkipped = true
	})
	return client, nil
}

func (f *FS) keepalive(ctx context.Context, client *s3.Client, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, every)
			_, err := client.HeadBucket(rctx, &s3.HeadBucketInput{Bucket: aws.String(f.Bucket)})
			cancel()
			if err != nil && ctx.Err() == nil {
				f.logger.Debug("s3cache keepalive failed", zap.Error(err))
			}
		}
	}
}

// Open implements fs.FS.
func (f *FS) Open(name string) (fs.File, error) {
	return f.cache.open(name)
}

// Stat implements fs.StatFS.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	return f.cache.stat(name)
}

// UnmarshalCaddyfile implements caddyfile.Unmarshaler.
//
//	filesystem <name> s3cache {
//	    bucket <name>
//	    region <region>
//	    endpoint <url>
//	    use_path_style
//	    prefix <key-prefix>
//	    cache_dir <path>
//	    cache_max_size <bytes>
//	    revalidate_after <duration>
//	    negative_ttl <duration>
//	    keepalive <duration>|off
//	    max_idle_conns <n>
//	}
func (f *FS) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	if !d.Next() { // skip the module name
		return d.ArgErr()
	}
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "bucket":
			if !d.AllArgs(&f.Bucket) {
				return d.ArgErr()
			}
		case "region":
			if !d.AllArgs(&f.Region) {
				return d.ArgErr()
			}
		case "endpoint":
			if !d.AllArgs(&f.Endpoint) {
				return d.ArgErr()
			}
		case "use_path_style":
			if d.NextArg() {
				return d.ArgErr()
			}
			f.UsePathStyle = true
		case "prefix":
			if !d.AllArgs(&f.Prefix) {
				return d.ArgErr()
			}
		case "cache_dir":
			if !d.AllArgs(&f.CacheDir) {
				return d.ArgErr()
			}
		case "cache_max_size":
			var raw string
			if !d.AllArgs(&raw) {
				return d.ArgErr()
			}
			n, err := parseSize(raw)
			if err != nil {
				return d.Errf("cache_max_size: %v", err)
			}
			f.CacheMaxSize = n
		case "revalidate_after", "negative_ttl", "keepalive":
			key := d.Val()
			var raw string
			if !d.AllArgs(&raw) {
				return d.ArgErr()
			}
			var dur time.Duration
			if raw == "off" || raw == "0" {
				if key != "keepalive" {
					return d.Errf("%s must be a positive duration", key)
				}
				dur = -1 // provisioned as "disabled", distinct from the zero default
			} else {
				parsed, err := caddy.ParseDuration(raw)
				if err != nil {
					return d.Errf("%s: %v", key, err)
				}
				if parsed <= 0 {
					return d.Errf("%s must be a positive duration", key)
				}
				dur = parsed
			}
			switch key {
			case "revalidate_after":
				f.RevalidateAfter = caddy.Duration(dur)
			case "negative_ttl":
				f.NegativeTTL = caddy.Duration(dur)
			case "keepalive":
				f.Keepalive = caddy.Duration(dur)
			}
		case "max_idle_conns":
			var raw string
			if !d.AllArgs(&raw) {
				return d.ArgErr()
			}
			var n int
			if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || n <= 0 {
				return d.Errf("max_idle_conns must be a positive integer")
			}
			f.MaxIdleConns = n
		default:
			return d.Errf("%s is not a valid caddy.fs.s3cache option", d.Val())
		}
	}
	return nil
}

// parseSize reads "512MiB", "1GiB", "100MB" or a plain byte count.
func parseSize(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	units := []struct {
		suffix string
		mult   int64
	}{
		{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10},
		{"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3},
		{"G", 1 << 30}, {"M", 1 << 20}, {"K", 1 << 10},
	}
	for _, u := range units {
		if strings.HasSuffix(raw, u.suffix) {
			var n float64
			if _, err := fmt.Sscanf(strings.TrimSuffix(raw, u.suffix), "%g", &n); err != nil || n <= 0 {
				return 0, fmt.Errorf("invalid size %q", raw)
			}
			return int64(n * float64(u.mult)), nil
		}
	}
	var n int64
	if _, err := fmt.Sscanf(raw, "%d", &n); err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	return n, nil
}
