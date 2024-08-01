package com.ibm.benchmark;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.ibm.benchmark.generator.JoinQueryGenerator;
import com.ibm.testbed.exporter.JdbcPrestoExporter;
import com.ibm.testbed.importer.DuckDbImporter;
import com.ibm.testbed.importer.Importer;
import com.ibm.testbed.importer.SamplerImportException;
import com.ibm.testbed.importer.SqliteImporter;
import com.ibm.testbed.tables.TPCHLineitem;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.math.Quantiles.percentiles;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public abstract class AbstractBenchmarkTest
{
    private static String path;
    private static final CustomLogger logger = new CustomLogger();

    private static long expectedCount;

    static final String DUCKDB_PATH = "/tmp/test_bench_duckdb.db";
    static final String SQLITE_PATH = "/tmp/test_bench_sqlite.db";

    private final String dbPath;
    private final String dbType;

    public AbstractBenchmarkTest(String dbType, String dbPath)
    {
        try {
            synchronized (this) {
                if (Objects.equals(path, null)) {
                    path = Files.createTempDirectory("presto_test_export").toFile().getAbsolutePath();
                    expectedCount = exportData();
                }
            }
            this.dbType = dbType;
            this.dbPath = dbPath;
        }
        catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    abstract String generateQuery();

    abstract String benchmarkName();

    public long exportData()
            throws SQLException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        JdbcPrestoExporter j = new JdbcPrestoExporter();
        Connection connection = j.getConnectionWithDefaults();
        expectedCount = j.exportTable(connection, path, "CSV", 2);
        sw.stop();
        logger.info("Presto CSV: Time to export {} records as CSV at path: {} : {}ms", expectedCount, path, sw.elapsed().toMillis());
        return expectedCount;
    }

    @Parameterized.Parameters
    public static Collection databasePaths()
    {
        return Arrays.asList(new String[][] {
                {"sqlite", SQLITE_PATH},
                {"duckdb", DUCKDB_PATH}
        });
    }

    @Test
    public void aImportData()
            throws SQLException, SamplerImportException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        Importer importer = null;
        if (dbType.equals("sqlite")) {
            importer = new SqliteImporter();
        }
        else if (dbType.equals("duckdb")) {
            importer = new DuckDbImporter();
        }
        assertNotNull("dbtype Not supported :" + dbType, importer);

        sw.reset();
        String dbUrl = String.format("jdbc:%s:%s", dbType, dbPath);
        try (Connection con = DriverManager.getConnection(dbUrl)) {
            con.setAutoCommit(false);
            String tableName = "TPCHLineitem";
            sw.start();
            importer.importFile(con, path, "CSV", tableName);
            sw.stop();
            logger.info("DbType: {}, Time to import {} records into '{}' : {}ms", dbType, expectedCount, dbUrl, sw.elapsed().toMillis());
            logger.info("DbType: {}, Size of the db file after importing the dataset {} bytes", dbType, FileUtils.sizeOf(new File(dbPath)));
            PreparedStatement statement = con.prepareStatement(String.format("SELECT * FROM %s LIMIT 1", tableName)); // To verify imported table has all columns
            ResultSet resultSet = statement.executeQuery();
            assertEquals("Imported table has missing columns", 16, resultSet.getMetaData().getColumnCount());
            statement = con.prepareStatement(String.format("SELECT count(*) FROM %s ", tableName));
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            assertEquals("mismatch in number of rows exported and number of rows imported", resultSet.getLong(1), expectedCount);
        }
    }

    // It is far from ideal to use junit framework for benchmarking
    // But this is our time marking solution. Need to make a proper benchmark suite.
    @Test
    public void testBenchmarkA()
            throws SQLException
    {
        List<Long> runTimes = new ArrayList<>();
        int zeroCount = 0;
        long sumRunTime = 0L;
        Stopwatch sw = Stopwatch.createUnstarted();
        String dbUrl = String.format("jdbc:%s:%s", dbType, dbPath);
        logger.info("DbType: {} , Benchmark: {}, Running like queries benchmark for Db with url : {} ", dbType, benchmarkName(), dbUrl);
        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            for (int i : IntStream.range(1, 100).toArray()) {
                String query = generateQuery();
                sw.reset();
                sw.start();
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                ResultSet resultSet = preparedStatement.executeQuery();
                sw.stop();
                if (resultSet.next()) {
                    long count = resultSet.getLong(1);
                    if (count == 0) {
                        zeroCount++;
                    }
                    sumRunTime += count;
                }
                runTimes.add(sw.elapsed().toMillis());
            }
        }
        Map<Integer, Double> nPercentiles =
                percentiles().indexes(25, 50, 99).compute(runTimes);
        logger.info("DbType: {} , Benchmark: {}, Concurrent: false, Average query running time: {}ms, n-percentiles: {}",
                dbType, benchmarkName(), runTimes.stream().reduce(0L, Long::sum) / (long) runTimes.size(),
                convertWithGuava(nPercentiles));
        logger.info("DbType: {} , Benchmark: {}, Concurrent: false, Average count sum : {}", dbType, benchmarkName(),
                sumRunTime / (long) runTimes.size());
        logger.info("DbType: {} , Benchmark: {}, Concurrent: false, No. of queries with zero count : {}", dbType,
                benchmarkName(), zeroCount);
    }

    @Test
    public void testBenchmarkBConcurrent()
            throws Exception
    {
        Queue<Long> runTimes = new ConcurrentLinkedQueue<Long>();
        List<Callable<Long>> callableTasks = new ArrayList<>();
        long sumRunTime = 0L;
        int concurrencyLevel = 4;
        try (ExecutorService executor = Executors.newFixedThreadPool(concurrencyLevel)) {
            for (int i : IntStream.range(0, concurrencyLevel).toArray()) {
                Callable<Long> callableTask = () -> {
                    long countSum = 0;
                    long count = 0;
                    try {
                        Connection connection = DriverManager.getConnection(String.format("jdbc:%s:%s", dbType, dbPath));
                        for (int j : IntStream.range(0, 20).toArray()) {
                            String query = generateQuery();
                            Stopwatch sw = Stopwatch.createUnstarted();
                            sw.start();
                            PreparedStatement preparedStatement = connection.prepareStatement(query);
                            ResultSet resultSet = preparedStatement.executeQuery();
                            sw.stop();
                            if (resultSet.next()) {
                                countSum += resultSet.getLong(1);
                                count++;
                            }
                            runTimes.add(sw.elapsed().toMillis());
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace(); // TODO not ideal, but okay for now.
                    }
                    return countSum / count;
                };
                callableTasks.add(callableTask);
            }
            List<Future<Long>> futures = executor.invokeAll(callableTasks);
            sumRunTime = futures.stream().map(s -> {
                try {
                    return s.get();
                }
                catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }).reduce(0L, Long::sum);
        }
        Map<Integer, Double> nPercentiles =
                percentiles().indexes(25, 50, 99).compute(runTimes);
        logger.info("DbType: {} , Benchmark: {}, Concurrency: 4 Average running time for a query : {}ms," +
                        " n-percentiles: {}", dbType, benchmarkName(),
                runTimes.stream().reduce(0L, Long::sum) / (long) runTimes.size(),
                convertWithGuava(nPercentiles));
        logger.info("DbType: {} , Benchmark: {}, Concurrency: 4 Average of selected records for a query", dbType, benchmarkName()
                , sumRunTime / concurrencyLevel);
    }

    public String convertWithGuava(Map<Integer, ?> map)
    {
        return Joiner.on(",").withKeyValueSeparator("=").join(map);
    }

    @Test
    public void zCleanup()
            throws IOException
    {
        // cleanup each test
        logger.info("Cleaning up! {} and dbPath : {}", path, dbPath);
        // FileUtils.deleteDirectory(new File(path));
        Files.deleteIfExists(Path.of(dbPath));
    }
}
