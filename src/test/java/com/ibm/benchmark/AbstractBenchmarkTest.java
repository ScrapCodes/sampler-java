package com.ibm.benchmark;

import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.ibm.config.Config;
import com.ibm.testbed.exporter.JdbcPrestoExporter;
import com.ibm.testbed.importer.Importer;
import com.ibm.testbed.importer.ImporterFactory;
import com.ibm.testbed.importer.SamplerImportException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public abstract class AbstractBenchmarkTest
{
    protected static final Config config = new Config();

    static {
        // JVM executes static function and blocks first.
        // Since Junit4 mandates static for certain functions.
        // This will ensure that config is loaded.
        try {
            config.loadViaJavaProperties();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Map<String, String> formatVsPathMap = new HashMap<>();
    private static final Map<String, Long> formatVsExpectedCountMap = new HashMap<>();

    private static final CustomLogger logger = new CustomLogger();

    protected final String jdbcUrl;
    protected final String dbType;
    protected final String format;

    public AbstractBenchmarkTest(String dbType, String jdbcUrl, String format)
    {
        try {
            synchronized (this) {
                if (!formatVsPathMap.containsKey(format)) {
                    String path = config.get(String.format("importer.source.%s.path", format.toLowerCase()));
                    if (path == null) {
                        path = Files.createTempDirectory(
                                        config.getConfWithDefault("benchmark.export.path.prefix", "presto_test_export"))
                                .toFile().getAbsolutePath();
                        long recordCount = exportData(path, format, 2);
                        config.setConf(String.format("importer.source.%s.count", format.toLowerCase()), recordCount);
                    }
                    formatVsPathMap.put(format, path);
                    long recordCount = Long.parseLong(config.getRequiredConf(String.format("importer.source.%s.count", format.toLowerCase())));
                    formatVsExpectedCountMap.put(format, recordCount);
                }
            }
            this.dbType = dbType;
            this.jdbcUrl = jdbcUrl;
            this.format = format;
        }
        catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    abstract String generateQuery();

    abstract String benchmarkName();

    String tableName()
    {
        return config.getTableName(dbType);
    }

    public long exportData(String path, String format, int samplePercent)
            throws SQLException, IOException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        JdbcPrestoExporter j = new JdbcPrestoExporter();
        Connection connection = j.getConnectionWithDefaults();
        long recCount = j.exportTable(connection, path, format, samplePercent);
        sw.stop();
        logger.info("Presto : Time to export {} records as {} at path: {} : {}ms", recCount, format, path, sw.elapsed().toMillis());
        return recCount;
    }

    @Parameterized.Parameters
    public static Collection<String[]> databasePaths()
    {
        List<String> dbTypes = Arrays.asList("duckdb", "sqlite", "prestodb");
        List<String[]> configList = new ArrayList<>();
        for (String db : dbTypes) {
            if (!config.getBoolean(String.format("tests.%s.skip", db))) {
                String format = "CSV";
                if (db.equalsIgnoreCase("prestodb")) {
                    format = "PARQUET";
                }
                configList.add(new String[] {db, config.getRequiredConf(String.format("importer.%s.jdbc_url", db)), format});
            }
        }
        return configList;
    }

    @Test
    public void aImportData()
            throws SQLException, SamplerImportException
    {
        if (config.getBoolean("importer.import_data.skip")) {
            return;
        }
        Stopwatch sw = Stopwatch.createUnstarted();
        Importer importer = ImporterFactory.createInstance(dbType);
        sw.reset();
        String path = formatVsPathMap.get(format);
        long expectedCount = formatVsExpectedCountMap.get(format);
        try (Connection con = importer.getConnection(jdbcUrl)) {
            sw.start();
            importer.importFile(con, path, format, tableName());
            sw.stop();
            logger.info("DbType: {}, Time to import {} records into '{}' : {}ms", dbType, expectedCount, jdbcUrl, sw.elapsed().toMillis());
            //logger.info("DbType: {}, Size of the db file after importing the dataset {} bytes", dbType, FileUtils.sizeOf(new File(jdbcUrl)));
            PreparedStatement statement = con.prepareStatement(String.format("SELECT * FROM %s LIMIT 1", tableName())); // To verify imported table has all columns
            ResultSet resultSet = statement.executeQuery();
            assertEquals("Imported table has missing columns", 16, resultSet.getMetaData().getColumnCount());
            statement = con.prepareStatement(String.format("SELECT count(*) FROM %s ", tableName()));
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            assertEquals("mismatch in number of rows exported and number of rows imported", resultSet.getLong(1), expectedCount);
        }
    }

    // It is far from ideal to use junit framework for benchmarking
    // But this is our time marking solution. Need to make a proper benchmark suite.
    @Test
    public void testBenchmarkA()
            throws SQLException, SamplerImportException
    {
        List<Long> runTimes = new ArrayList<>();
        int zeroCount = 0;
        long sumRunTime = 0L;
        Stopwatch sw = Stopwatch.createUnstarted();
        Importer importer = ImporterFactory.createInstance(dbType);
        logger.info("DbType: {} , Benchmark: {}, Db with url : {} ", dbType, benchmarkName(), jdbcUrl);
        try (Connection connection = importer.getConnection(jdbcUrl)) {
            for (int i : IntStream.range(1, 100).toArray()) {
                String query = generateQuery();
                sw.reset();
                sw.start();
                PreparedStatement preparedStatement = connection.prepareStatement(query);
                //                         .put("http-server.max-request-header-size", "50MB") is required to be added to coordinator config
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

        Map<Integer, Double> nPercentiles = percentiles().indexes(25, 50, 99).compute(runTimes);
        logger.info("DbType: {} , Benchmark: {}, Concurrent: false, Average query running time: {}ms, n-percentiles: {}", dbType, benchmarkName(), runTimes.stream().reduce(0L, Long::sum) / (long) runTimes.size(), convertWithGuava(nPercentiles));
        logger.info("DbType: {} , Benchmark: {}, Concurrent: false, Average count sum : {}", dbType, benchmarkName(), sumRunTime / (long) runTimes.size());
        logger.info("DbType: {} , Benchmark: {}, Concurrent: false, No. of queries with zero count : {}", dbType, benchmarkName(), zeroCount);
    }

    @Test
    public void testBenchmarkBConcurrent()
            throws Exception
    {
        Queue<Long> runTimes = new ConcurrentLinkedQueue<Long>();
        List<Callable<Long>> callableTasks = new ArrayList<>();
        long sumRunTime = 0L;
        int concurrencyLevel = 4;
        Importer importer = ImporterFactory.createInstance(dbType);
        try (ExecutorService executor = Executors.newFixedThreadPool(concurrencyLevel)) {
            for (int i : IntStream.range(0, concurrencyLevel).toArray()) {
                Callable<Long> callableTask = () -> {
                    long countSum = 0;
                    long count = 0;
                    try {
                        try (Connection connection = importer.getConnection(jdbcUrl)) {
                            for (int j : IntStream.range(0, 25).toArray()) {
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
        Map<Integer, Double> nPercentiles = percentiles().indexes(25, 50, 99).compute(runTimes);
        logger.info("DbType: {} , Benchmark: {}, Concurrency: {} Average running time for a query : {}ms, n-percentiles: {}", dbType, benchmarkName(), concurrencyLevel, runTimes.stream().reduce(0L, Long::sum) / (long) runTimes.size(), convertWithGuava(nPercentiles));
        logger.info("DbType: {} , Benchmark: {}, Concurrency: {} Average of selected records for a query", dbType, benchmarkName(), concurrencyLevel, sumRunTime / concurrencyLevel);
    }

    private String convertWithGuava(Map<?, ?> map)
    {
        return Joiner.on(",").withKeyValueSeparator("=").join(map);
    }

    @Test
    public void zCleanup()
            throws IOException
    {
        // cleanup each test
        logger.info("Cleaning up! {} and dbPath : {}", convertWithGuava(formatVsPathMap), jdbcUrl);
        // FileUtils.deleteDirectory(new File(path));
        // Files.deleteIfExists(Path.of(jdbcUrl));
    }
}
