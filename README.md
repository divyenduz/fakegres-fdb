# Fakegres

Distributed PostgreSQL backed by FoundationDB.

## Setup

Setup FoundationDB (https://apple.github.io/foundationdb/) on your machine.

```bash
$ go mod tidy
$ go build
$ ./fakegres-fdb -pg-port=6000 -reset=false -columnar=false
$ psql -h localhost -p 6000

psql> create table customer (age int, name text);
psql> insert into customer values(14, 'garry'), (20, 'ted');
psql> select name, age from customer;
```

## Introduction

This builds on top of [Fakegres + SQLite](https://github.com/divyenduz/fakegres) ([tweet](https://x.com/divyenduz/status/1759917106743693580)).

Basically, this is 0.00001 version of SQL over KV, the idea is not new at all. CockroadchDB does it in production and have [heavily documented it](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go). and even Foundation DB had an [SQL layer](https://forums.foundationdb.org/t/sql-layer-in-foundationdb/94/3) that is not longer maintained.

I wanted to learn data modeling with Foundation DB and this was one my [didn't get to it projects at Recurse](https://blog.divyendusingh.com/p/recurse-center-return-statement).

The code is heavily commented to show my intent, needless to say this is very WIP.

## Resources

- [Data modeling in Foundation DB](https://apple.github.io/foundationdb/data-modeling.html)
- [The architecture of a distributed SQL database, part 1: Converting SQL to a KV store](https://www.cockroachlabs.com/blog/distributed-sql-key-value-store/)
- [CockroachDB: Architecture of a Geo-Distributed SQL Database](https://youtu.be/OJySfiMKXLs?t=1104)
- [CockroachDB's v3 encoding](https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go)
- [CockroachDB's v2 encoding](https://www.cockroachlabs.com/blog/sql-cockroachdb-column-families/)
- [CockroachDB's v1 encoding](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)
- [FoundationDB SQL Layer (community)](https://github.com/qiukeren/foundationdb-sql-layer)
- [What's the big deal about key-value databases like FoundationDB and RocksDB?](https://notes.eatonphil.com/whats-the-big-deal-about-key-value-databases.html)
