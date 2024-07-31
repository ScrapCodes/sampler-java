package com.ibm.testbed.importer;

import com.google.common.base.Stopwatch;
import com.ibm.benchmark.JoinQueryGenerator;
import com.ibm.testbed.exporter.JdbcPrestoExporter;
import com.ibm.testbed.tables.TPCHLineitem;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractImporterTest
{ // It is far from ideal to use junit framework for benchmarking
    // But this is our time marking solution. Need to make a proper benchmark suite.
    static String path;

    static {
        try {
            path = Files.createTempDirectory("presto_test_export").toFile().getAbsolutePath();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static long expectedCount;

    String dbPath = String.format("/tmp/test_%s.db", getDbType());

    abstract String getDbType();

    @BeforeClass
    public static void exportData()
            throws SQLException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        JdbcPrestoExporter j = new JdbcPrestoExporter();
        Connection connection = j.getConnectionWithDefaults();
        expectedCount = j.exportTable(connection, path, "CSV", 2);
        sw.stop();
        System.out.printf("\nTime to export %d records as CSV %s : %dms", expectedCount, path, sw.elapsed().toMillis());
    }

    protected void importData(Importer importer, String dbType)
            throws SQLException, SamplerImportException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.reset();
        String dbUrl = String.format("jdbc:%s:%s", getDbType(), dbPath);
        try (Connection con = DriverManager.getConnection(dbUrl)) {
            con.setAutoCommit(false);
            String tableName = "TPCHLineitem";
            sw.start();
            importer.importFile(con, path, "CSV", tableName);
            sw.stop();
            System.out.printf("\nTime to import %d records into '%s' : %dms", expectedCount, dbUrl, sw.elapsed().toMillis());
            System.out.printf("\n Size of the %s db file after importing the dataset %d bytes", dbType, FileUtils.sizeOf(new File(dbPath)));
            PreparedStatement statement = con.prepareStatement(String.format("SELECT * FROM %s LIMIT 1", tableName)); // To verify imported table has all columns
            ResultSet resultSet = statement.executeQuery();
            assertEquals("Imported table has missing columns", 16, resultSet.getMetaData().getColumnCount());
            statement = con.prepareStatement(String.format("SELECT count(*) FROM %s ", tableName));
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            assertEquals("mismatch in number of rows exported and number of rows imported", resultSet.getLong(1), expectedCount);
        }
    }

    @Test
    public void testBenchmarkALikeQueries()
            throws SQLException
    {
        List<Long> runTimes = new ArrayList<>();
        int zeroCount = 0;
        long sumRunTime = 0L;
        Stopwatch sw = Stopwatch.createUnstarted();
        String dbUrl = String.format("jdbc:%s:%s", getDbType(), dbPath);
        System.out.println("\nRunning like queries benchmark for Db with url : " + dbUrl);
        try (Connection connection = DriverManager.getConnection(dbUrl)) {
            for (int i : IntStream.range(1, 100).toArray()) {
                String query = JoinQueryGenerator.generateRandomLikeQuery(TPCHLineitem.class);
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
        System.out.printf("\nAverage running time: %dms", runTimes.stream().reduce(0L, Long::sum) / (long) runTimes.size());
        System.out.printf("\nAverage count sum : %d", sumRunTime / (long) runTimes.size());
        System.out.printf("\n No. of queries with zero count : %d", zeroCount);
    }

    @Test
    public void testBenchmarkBLikeQueriesConcurrent()
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
                        Connection connection = DriverManager.getConnection(String.format("jdbc:%s:%s", getDbType(), dbPath));
                        for (int j : IntStream.range(0, 20).toArray()) {
                            String query = JoinQueryGenerator.generateRandomLikeQuery(TPCHLineitem.class);
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
        System.out.printf("\nAverage running time for concurrent queries with concurrency: %d is : %dms", concurrencyLevel,
                runTimes.stream().reduce(0L, Long::sum) / (long) runTimes.size());
        System.out.printf("\nAverage count sum for concurrent queries with concurrency: %d is : %d", concurrencyLevel, sumRunTime / concurrencyLevel);
    }

    @Test
    public void zCleanup()
            throws IOException
    {
        // cleanup each test
        Files.deleteIfExists(Path.of(dbPath));
    }

    @AfterClass
    public static void cleanup()
            throws IOException
    {
        // Find a way this is run only once and not for every subclass of test.
//        System.out.println("\nCleaning up! : " + path);
//        FileUtils.deleteDirectory(new File(path));
    }
}
