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

1. For each benchmark category (except import/export) 100 trials were done and times are average.
2. A 2% sample was selected at random using Bernoulli sampling of `lineitem` table with sf `SF10`, it has approximately 1 million records.
3. Each benchmark was run in sequential as well as concurrent way. Where the concurrency count is 4 on a Baremetal system with 20 Virtual cores, 64GB memory and 1TB NVMe.

## Results

Results with 2% Sample.

| Benchmark Name                     | sqlite                              | duckdb                         | Prestodb Native (parquet)             | presto_export | mysql | presto_import |
|------------------------------------|-------------------------------------|--------------------------------|---------------------------------------|---------------|-------|---------------|
| import_1M_records (1199495)        | 15946ms                             | 2753ms                         | 322ms                                 | 6090ms        |       |               |
| count_distinct_query               | 589ms (25=83.5,50=100.0,99=2327.5)  | 5ms (25=2.0,50=3.0,99=33.04)   | 182ms (25=82.0,50=147.0,99=592.16)    | ?             |       |               |
| count_distinct_query_concurrency_4 | 667ms (25=90.75,50=109.5,99=2850.0) | 20ms (25=4.75,50=8.0,99=90.89) | 323ms (25=111.75,50=229.0,99=1063.15) | ?             |       |               |
| is_null_query (0 records selected) | 59ms (25=54.0,50=59.0,99=65.02)     | 0ms (25=0.0,50=0.0,99=0.0)     | 81ms  (25=45.0,50=48.0,99=136.04)     |               |       |               |
| is_null_query_concurrency_4        | 68ms (25=58.0,50=62.0,99=70.0)      | 0ms (25=0.0,50=0.0,99=0.0)     | 66ms  (25=62.0,50=65.0,99=88.03)      |               |       |               |
| join_like_query                    | 98ms  (25=83.5,50=90.0,99=141.08)   | 3ms (25=2.0,50=3.0,99=10.0)    | 120ms (25=58.0,50=143.0,99=261.04)    |               |       |               |
| join_like_query_concurrency_4      | 100ms (25=91.0,50=97.0,99=141.68)   | 7ms (25=4.0,50=6.0,99=20.84)   | 141ms (25=81.0,50=94.0,99=443.17)     |               |       |               |
| Database size                      | 145924096 bytes                     | 36712448 bytes                 | N/A                                   | 236 Mb        |       |               |

Following results with 20% sample.

| Benchmark Name                     | sqlite                                   | duckdb                               | presto_export | mysql | presto_import |
|------------------------------------|------------------------------------------|--------------------------------------|---------------|-------|---------------|
| import_1M_records (11994472)       | 129557ms                                 | 7766ms                               | 30130ms       |       |               |
| count_distinct_query               | 9644ms (25=823.0,50=1006.0,99=30604.38)  | 68ms (25=8.0,50=13.0,99=317.42)      | ?             |       |               |
| count_distinct_query_concurrency_4 | 9114ms (25=911.25,50=1088.5,99=41368.74) | 270ms (25=55.75,50=102.0,99=1148.01) | ?             |       |               |
| is_null_query (0 records selected) | 585ms (25=551.0,50=585.0,99=659.46)      | 0ms (25=0.0,50=0.0,99=0.0)           |               |       |               |
| is_null_query_concurrency_4        | 720ms (25=661.0,50=715.0,99=863.1)       | 0ms (25=0.0,50=0.0,99=0.0)           |               |       |               |
| join_like_query                    | 1043ms  (25=887.0,50=996.0,99=1475.48)   | 20ms (25=12.0,50=14.0,99=50.02)      |               |       |               |
| join_like_query_concurrency_4      | 1090ms (25=926.75,50=1072.0,99=1482.25)  | 107ms (25=64.75,50=85.0,99=281.99)   |               |       |               |
| Database size                      | 1469136896 bytes                         | 361246720 bytes                      | 2.3 G         |       |               |
