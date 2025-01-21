# sqliteutils

Pooling, querying, backup, and testing utilities for [zombiezen/go-sqlite](/zombiezen/go-sqlite).

## Install

Download from [Releases](https://github.com/dropsite-ai/sqliteutils/releases):

```bash
tar -xzf sqliteutils_Darwin_arm64.tar.gz
chmod +x sqliteutils
sudo mv sqliteutils /usr/local/bin/
```

Or manually build and install:

```bash
git clone git@github.com:dropsite-ai/sqliteutils.git
cd sqliteutils
make install
```

## Test

```bash
make test
```

## Release

```bash
make release
```