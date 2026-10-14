// SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
// SPDX-License-Identifier: MIT

package s3cache

import (
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

func TestUnmarshalCaddyfile(t *testing.T) {
	d := caddyfile.NewTestDispenser(`s3cache {
		bucket koh-web-tenant-media
		region eu-north-1
		endpoint http://s3:9000
		use_path_style
		prefix media
		cache_dir /var/cache/fileadmin
		cache_max_size 1GiB
		revalidate_after 2m
		negative_ttl 15s
		keepalive off
		max_idle_conns 8
	}`)
	var f FS
	if err := f.UnmarshalCaddyfile(d); err != nil {
		t.Fatal(err)
	}
	if f.Bucket != "koh-web-tenant-media" || f.Region != "eu-north-1" || f.Endpoint != "http://s3:9000" || !f.UsePathStyle {
		t.Fatalf("connection fields: %+v", f)
	}
	if f.Prefix != "media" || f.CacheDir != "/var/cache/fileadmin" || f.CacheMaxSize != 1<<30 {
		t.Fatalf("cache fields: %+v", f)
	}
	if time.Duration(f.RevalidateAfter) != 2*time.Minute || time.Duration(f.NegativeTTL) != 15*time.Second {
		t.Fatalf("durations: %+v", f)
	}
	if f.Keepalive >= 0 {
		t.Fatalf("keepalive off must be recorded as disabled, got %v", f.Keepalive)
	}
	if f.MaxIdleConns != 8 {
		t.Fatalf("max_idle_conns: %d", f.MaxIdleConns)
	}
}

func TestUnmarshalCaddyfileRejectsUnknownAndInvalid(t *testing.T) {
	cases := map[string]string{
		"unknown option": `s3cache { bucket b
 force_path_style }`,
		"two bucket args": `s3cache { bucket a b }`,
		"bad size": `s3cache { bucket b
 cache_max_size huge }`,
		"zero revalidate": `s3cache { bucket b
 revalidate_after 0 }`,
		"negative idle conns": `s3cache { bucket b
 max_idle_conns -1 }`,
		"use_path_style w/ arg": `s3cache { bucket b
 use_path_style yes }`,
	}
	for name, src := range cases {
		var f FS
		if err := f.UnmarshalCaddyfile(caddyfile.NewTestDispenser(src)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestProvisionAppliesDefaults(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: t.Context()})
	defer cancel()
	f := FS{Bucket: "b", Region: "eu-north-1", Endpoint: "http://127.0.0.1:1", UsePathStyle: true, CacheDir: t.TempDir()}
	t.Setenv("AWS_ACCESS_KEY_ID", "x")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "y")
	if err := f.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Cleanup() }()
	if f.CacheMaxSize != DefaultCacheMaxSize || time.Duration(f.RevalidateAfter) != DefaultRevalidateAfter ||
		time.Duration(f.NegativeTTL) != DefaultNegativeTTL || time.Duration(f.Keepalive) != DefaultKeepalive || f.MaxIdleConns != DefaultMaxIdleConns {
		t.Fatalf("defaults not applied: %+v", f)
	}
	if f.cache == nil {
		t.Fatal("cache not created")
	}
}

func TestProvisionRequiresBucket(t *testing.T) {
	ctx, cancel := caddy.NewContext(caddy.Context{Context: t.Context()})
	defer cancel()
	var f FS
	if err := f.Provision(ctx); err == nil {
		t.Fatal("expected an error without bucket")
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{"512MiB": 512 << 20, "1GiB": 1 << 30, "100MB": 100_000_000, "2048": 2048, "1.5G": 3 << 29}
	for in, want := range cases {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("%s: got %d %v, want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "0", "-1", "xMiB", "MiB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}
