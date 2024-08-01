# Sampler

## Run tests

`mvn test`

## How the benchmark was run.

1. For each benchmark category (except import/export) 100 trials were done and times are average.
2. A 2% sample was selected at random using Bernoulli sampling of `lineitem` table with sf `SF10`, it has approximately 1 million records.
3. Each benchmark was run in sequential as well as concurrent way. Where the concurrency count is 4 on a Baremetal system with 20 Virtual cores, 64GB memory and 1TB NVMe.


## Results

| Benchmark Name                     | sqlite          | duckdb          | presto_export | mysql | presto_import |
|------------------------------------|-----------------|-----------------|---------------|-------|---------------|
| import_1M_records (119995)         | 15946ms         | 2753ms          | 6090ms        |       |               |
| count_distinct_query               | 589ms           | 5ms             |               |       |               |
| count_distinct_query_concurrency_4 | 667ms           | 20ms            |               |       |               |
| is_null_query                      | 59ms            | 0ms (0 records) |               |       |               |
| is_null_query_concurrency_4        | 68ms            | 0ms (0 records) |               |       |               |
| join_like_query                    | 98ms            | 3ms             |               |       |               |
| join_like_query_concurrency_4      | 100ms           | 7ms             |               |       |               |
| Database size                      | 145924096 bytes | 36712448 bytes  | 236 Mb        |       |               |