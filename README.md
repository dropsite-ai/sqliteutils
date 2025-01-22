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

## Usage

```bash
  -dbpath string
    	Path to the SQLite database file (default "sqlite.db")
  -poolsize int
    	Number of connections in the pool (default 4)
  -query string
    	SQL query to execute (default "SELECT sqlite_version();")
```

## Test

```bash
make test
```

## Release

```bash
make release
```