<!--
SPDX-FileCopyrightText: 2026 Kai Ole Hartwig
SPDX-License-Identifier: MIT
-->

# Contributing

Issues and merge requests are welcome.

## Where to file things

Development happens on **GitLab**: <https://git.ole-hartwig.eu/ai-ready-platform/platform>.
The GitHub repository is a read-only mirror — it exists so Packagist has a public
remote to read. Issues and pull requests opened there will not be seen. Please
use the GitLab issue tracker linked from `composer.json`.

## Developer Certificate of Origin

Every commit must be signed off. By adding the line

```text
Signed-off-by: Your Name <your.email@example.com>
```

you certify the [Developer Certificate of Origin 1.1](https://developercertificate.org/):
that you wrote the patch or have the right to submit it under the project's licence.

`git commit -s` adds the line for you.

## Before you open a merge request

```bash
composer install
vendor/bin/phpunit
vendor/bin/phpstan analyse
vendor/bin/phpunit -c Tests/FunctionalTests.xml   # SQLite, no setup needed
composer validate --strict
```

All of them must pass. PHPStan runs at level `max` and the pipeline blocks on it.

Every source file carries an SPDX header:

```php
// SPDX-FileCopyrightText: 2026 Your Name
// SPDX-License-Identifier: MIT
```

## What makes a good change here

This package is read by people trying to understand a protocol.
A comment explaining **why** a decision was made is worth more than one restating
what the line does — most of the non-obvious code here exists because the
specification, the search engines and the framework disagreed, and the next reader needs to
know which one won and why.

Add a test that pins the decision rather than the mechanism.
`aRestrictedPageIsNeverAnnouncedNotEvenWhenItChanges` says something a future
maintainer needs; `testProcessorReturnsReport` does not.

## Reporting a vulnerability

Do not open a public issue. See [SECURITY.md](SECURITY.md).
