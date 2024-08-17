# Sampler

## Run tests

### Add configuration for running the benchmark

Modify sampler.properties file to reflect the correct values.

```properties
# Table imported data is written to.
importer.prestodb.table=lineitem3
importer.duckdb.table=lineitem3
importer.sqlite.table=lineitem3
# JDBC url
importer.prestodb.jdbcUrl=jdbc:presto://127.0.0.1:8081/hive/tpch
importer.duckdb.jdbcUrl=jdbc:duckdb:/tmp/test_bench_duckdb.db
importer.sqlite.jdbcUrl=jdbc:sqlite:/tmp/test_bench_sqlitedb.db
# Importer
importer.source.parquet.path=/tmp/presto_test_export7612448730617712277
importer.source.csv.path=/tmp/presto_test_export5567570570163389490
importer.source.parquet.count=1200121
importer.source.csv.count=1200028
importer.skip=true
```

`mvn test`

## How the benchmark was run.

1. For each benchmark category (except import/export) 1000 trials were done and times are average.
2. A 2% sample was selected at random using Bernoulli sampling of `lineitem` table with sf `SF10`, it has approximately 1 million records.
3. Each benchmark was run in sequential as well as concurrent way. Where the concurrency count is 4 on a Baremetal system with 20 Virtual cores, 64GB memory and 1TB NVMe.

## Results

Results with 2% Sample.

| Benchmark Name                     | sqlite                              | duckdb                          | Prestodb Native (parquet)           | presto_export | mysql | presto_import |
|------------------------------------|-------------------------------------|---------------------------------|-------------------------------------|---------------|-------|---------------|
| import_1M_records (1199495)        | 15946ms                             | 2753ms                          | 322ms                               | 6090ms        |       |               |
| count_distinct_query               | 569ms (25=87.0,50=104.0,99=2532.08) | 8ms (25=2.0,50=3.0,99=41.0)     | 112ms (25=44.0,50=46.0,99=388.02)   | ?             |       |               |
| count_distinct_query_concurrency_4 | 662ms (25=98.0,50=115.0,99=3159.01) | 26ms (25=6.0,50=11.0,99=127.01) | 378ms (25=88.0,50=128.5,99=1858.13) | ?             |       |               |
| is_null_query (0 records selected) | 60ms (25=56.0,50=61.0,99=68.0)      | 0ms (25=0.0,50=0.0,99=0.0)      | 36ms  (25=35.0,50=36.0,99=63.0)     |               |       |               |
| is_null_query_concurrency_4        | 65ms (25=62.0,50=66.0,99=78.01)     | 0ms (25=0.0,50=0.0,99=0.0)      | 72ms  (25=65.0,50=68.0,99=96.0)     |               |       |               |
| join_like_query                    | 98ms  (25=87.0,50=92.0,99=141.0)    | 4ms (25=2.0,50=3.0,99=11.0)     | 86ms (25=51.0,50=53.0,99=236.0)     |               |       |               |
| join_like_query_concurrency_4      | 102ms (25=90.0,50=96.0,99=147.0)    | 10ms (25=6.75,50=9.0,99=30.0)   | 149ms (25=92.0,50=100.0,99=411.03)  |               |       |               |
| Database size                      | 145924096 bytes                     | 36712448 bytes                  | N/A                                 | 236 Mb        |       |               |

Following results with 20% sample.

| Benchmark Name                     | sqlite                                   | duckdb                              | Prestodb Native (parquet)               | presto_export | mysql | presto_import |
|------------------------------------|------------------------------------------|-------------------------------------|-----------------------------------------|---------------|-------|---------------|
| import_10M_records (11998749)      | 129557ms                                 | 7766ms                              | 322ms                                   | 30130ms       |       |               |
| count_distinct_query               | 9644ms (25=823.0,50=1006.0,99=30604.38)  | 83ms (25=9.0,50=16.0,99=368.02)     | 1756ms (25=120.0,50=123.0,99=9829.1)    | ?             |       |               |
| count_distinct_query_concurrency_4 | 9114ms (25=911.25,50=1088.5,99=41368.74) | 277ms (25=66.0,50=107.0,99=1306.02) | 5430ms (25=709.75,50=985.5,99=26854.96) | ?             |       |               |
| is_null_query (0 records selected) | 585ms (25=551.0,50=585.0,99=659.46)      | 0ms (25=0.0,50=0.0,99=0.0)          | 31ms  (25=30.0,50=31.0,99=51.0)         |               |       |               |
| is_null_query_concurrency_4        | 720ms (25=661.0,50=715.0,99=863.1)       | 0ms (25=0.0,50=0.0,99=0.0)          | 50ms  (25=46.0,50=49.0,99=67.0)         |               |       |               |
| join_like_query                    | 1043ms  (25=887.0,50=996.0,99=1475.48)   | 28ms (25=16.0,50=18.0,99=73.0)      | 556ms (25=176.0,50=190.0,99=2032.02)    |               |       |               |
| join_like_query_concurrency_4      | 1090ms (25=926.75,50=1072.0,99=1482.25)  | 97ms (25=64.0,50=81.0,99=249.02)    | 1039ms (25=358.75,50=410.0,99=3787.18)  |               |       |               |
| Database size                      | 1469136896 bytes                         | 361246720 bytes                     | N/A                                     | 2.3 G         |       |               |

**Planning time** was 29ms for first query and then for next 30-40 queries 1-4ms and 0ms for rest of 6k queries. ( No. of experiments: 1K for each query/benchmark type).

Other observations:

1. During benchmark sqlite CPU utilization is not multithreaded.
2. Duckdb achieves 100% cpu utilization including all 20 virtual cores in concurrent query mode and around 60-90% during query at a time benchmark. Memory usage ranges from 1.5Gb to 1.8Gb.
3. Prestodb native worker does not utilize more than 3.1 GB of memory, even in the 10M rows benchmark, in-spite of setting a high memory limit (~40Gb). Its CPU utilization remains at 20% during a query at a time benchmark and
   increases to 70-80% during concurrent query benchmark. Disk i/o: 344Kb total disk read and 0kb write.

Configuration used for native worker:

```properties
discovery.uri=http://127.0.0.1:33287
presto.version=testversion
http-server.http.port=7777
shutdown-onset-sec=1
register-test-functions=true
runtime-metrics-collection-enabled=false
async-data-cache-enabled=true
memory-arbitrator-global-arbitration-enabled=false
memory-arbitrator-kind=SHARED
system-memory-gb=40
query-memory-gb=28
system-mem-pushback-abort-enabled=true
cache.velox.ttl-enabled=false
skip-runtime-stats-in-running-task-info=true
driver.num-cpu-threads-hw-multiplier=0.8
connector.num-io-threads-hw-multiplier=0.5
http-server.num-cpu-threads-hw-multiplier=0.25
http-server.num-io-threads-hw-multiplier=0.25
spiller.num-cpu-threads-hw-multiplier=0.0
exchange.http-client.num-io-threads-hw-multiplier=0.5
exchange.http-client.num-cpu-threads-hw-multiplier=0.5
task.max-drivers-per-task=6
max_split_preload_per_driver=25
enable-serialized-page-checksum=false
```