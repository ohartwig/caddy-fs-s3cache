<!--
SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
SPDX-License-Identifier: MIT
-->

# caddy-fs-s3cache

A Caddy virtual filesystem (`caddy.fs.s3cache`) that serves objects from an
S3 bucket through a local disk cache. Built for `file_server` in front of a
media bucket, where every request used to cost three S3 round trips and
nothing was remembered between requests.

## Why

`file_server` on a plain S3 filesystem (for example `caddy.fs.s3`, which
wraps `jszwec/s3fs`) performs, per served file:

1. `Stat` → `HeadObject`
2. `Open` → `GetObject`
3. `http.ServeContent` seeks to the end and back → the running `GetObject`
   is aborted and a second `GetObject` is issued

Measured in a TYPO3 tenant in eu-north-1, that is 73–85 ms for a 900-byte
image, 51 ms for a miss, against 3 ms from local disk. This module fetches an
object with **one** `GetObject`, keeps the body on local disk, answers `Stat`,
`Open`, `Seek` and `ReadAt` from that copy, and asks S3 again only after
`revalidate_after`, with `If-None-Match` (a 304 moves no body).

| Case | S3 requests | Typical latency (in-region) |
|---|---|---|
| hot object (within `revalidate_after`) | 0 | < 1 ms |
| stale object, unchanged upstream | 1 (304) | one round trip |
| cold or changed object | 1 (200) | one round trip + body |
| missing key (within `negative_ttl`) | 1, then 0 | one round trip, then < 1 ms |
| 32 concurrent first requests for one key | 1 | shared |

## Usage

```caddyfile
{
    filesystem media s3cache {
        bucket my-media-bucket
        region eu-north-1
        # endpoint http://s3:9000      # S3-compatible stores
        # use_path_style
        # prefix media/                # key prefix for every path
        cache_dir /var/cache/caddy/media
        cache_max_size 512MiB
        revalidate_after 60s
        negative_ttl 30s
        keepalive 25s                  # background HeadBucket; `off` to disable
        max_idle_conns 16
    }
}

example.org {
    handle /media/* {
        route {
            uri strip_prefix /media
            root * .
            fs media
            file_server {
                pass_thru
            }
            respond "Not Found" 404
        }
    }
}
```

Credentials come from the AWS SDK default chain: environment, shared config,
web identity (IRSA / `AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`), instance
metadata. The module needs `s3:GetObject` on the keys and, for `keepalive`,
`s3:ListBucket` on the bucket.

### Options

| Option | Default | Meaning |
|---|---|---|
| `bucket` | required | S3 bucket |
| `region` | SDK default | AWS region |
| `endpoint` | AWS | custom endpoint (RustFS, MinIO, …) |
| `use_path_style` | off | path-style addressing |
| `prefix` | none | prepended to every key; a trailing `/` is added if missing |
| `cache_dir` | `$TMPDIR/caddy-fs-s3cache/<bucket>` | body files; emptied when the module is provisioned |
| `cache_max_size` | `512MiB` | LRU bound on cached bytes (`KiB`/`MiB`/`GiB`/`KB`/`MB`/`GB` or bytes) |
| `revalidate_after` | `60s` | serve from cache this long before a conditional GET |
| `negative_ttl` | `30s` | remember a 404 this long |
| `keepalive` | `25s` | interval of a `HeadBucket` that keeps the TLS connection warm; `off` disables |
| `max_idle_conns` | `16` | pooled connections to S3 |

### Semantics worth knowing

- `Stat` and `Open` are answered from the same cached copy, so `Last-Modified`
  and Caddy's ETag (mtime + size) follow the object's `Last-Modified`.
- A replaced object (same key, new ETag) becomes visible at the next
  revalidation, at most `revalidate_after` after the write. A deleted object
  turns into a 404 the same way, and its body is evicted.
- When S3 fails (network, 5xx, 403) and a cached copy exists, the copy is
  served and a warning is logged. Without a copy, the error propagates and
  `file_server` answers 500 rather than a misleading 404.
- The root `.` is a synthetic directory; everything else is treated as an
  object key. Paths that are not valid `fs.FS` names (`..`, leading `/`,
  trailing `/`) are rejected without touching S3.
- `cache_dir` must be private to one module instance. In Kubernetes an
  `emptyDir` is the right volume: the index lives in memory and starts empty,
  so the directory is wiped on provision.

## Development

```sh
go test ./...
go run github.com/caddyserver/xcaddy/cmd/xcaddy@latest build v2.11.7 \
  --with github.com/ohartwig/caddy-fs-s3cache=. --output ./caddy
```

Unit tests run against an in-process fake S3 and count requests. For an
end-to-end run, point `endpoint` at a local RustFS or MinIO.

## Licence

MIT, see [LICENSE](LICENSE).
