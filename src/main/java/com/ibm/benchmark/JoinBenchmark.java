package com.ibm.benchmark;

import com.ibm.benchmark.generator.JoinQueryGenerator;
import com.ibm.benchmark.generator.QueryGenerator;
import com.ibm.testbed.importer.DuckDbImporter;
import com.ibm.testbed.importer.SqliteImporter;
import com.ibm.testbed.importer.SamplerImportException;
import com.ibm.testbed.tables.TPCHLineitem;
import org.apache.commons.io.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * This is an evaluation of JMH framework is a good candidate for benchmarking or not.
 * *Somehow JMH did not meet the needs for benchmark.*
 * Actual benchmarks are written in Junit tests for now, eventually, we will have something standalone.
 */
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
public class JoinBenchmark
{
    static QueryGenerator qg = new JoinQueryGenerator();

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.Throughput})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public String generateQueries()
    {
        return qg.generateRandomQuery(TPCHLineitem.class);
    }

    @State(Scope.Benchmark)
    public static class PrestoExportedData
    {
        String tmpDir;

        @Setup()
        public void setup()
                throws SQLException, IOException
        {
            tmpDir = "/tmp/presto_export6187521336659539075";
//                    Files.createTempDirectory("presto_export").toFile().getAbsolutePath();
//            JdbcPrestoExporter j = new JdbcPrestoExporter();
//            try (Connection connection = j.getConnectionWithDefaults()) {
//                j.exportTable(connection, tmpDir, "CSV");
//            }
            // Utils.deleteFilesByWildChar(basePath, ".*.crc");
        }

        @TearDown
        public void tearDown()
                throws IOException, SQLException
        {
            //  FileUtils.deleteDirectory(new File(tmpDir));
        }
    }

    @State(Scope.Benchmark)
    public static class SqliteQuery
    {
        String query;
        Connection sqliteCon;
        String tmpDir;
        String sqliteDb;

        @Setup()
        public void setup1(PrestoExportedData prestoExportedData)
                throws SamplerImportException, SQLException, IOException
        {
            tmpDir = Files.createTempDirectory("sqlite").toFile().getAbsolutePath();
            sqliteDb = tmpDir + "/test_sqlite.db";
            // Import the table in a Sqlite db
            SqliteImporter sqLiteImporter = new SqliteImporter();
            sqliteCon = sqLiteImporter.getConnection(sqliteDb);
            sqliteCon.setAutoCommit(false);
            sqLiteImporter.importFile(sqliteCon, prestoExportedData.tmpDir, "csv", TPCHLineitem.class.getSimpleName());
        }

        @TearDown
        public void tearDown()
                throws IOException, SQLException
        {
            sqliteCon.commit();
            sqliteCon.close();
            FileUtils.deleteDirectory(new File(tmpDir));
        }

        // This causes query string to be generated/invocation and the cost of generation is not included in measurements.
        @Setup(Level.Invocation)
        public void setup()
        {
            query = qg.generateRandomQuery(TPCHLineitem.class);
        }
    }

    @State(Scope.Benchmark)
    public static class DuckDbQuery
    {
        String query;
        Connection duckDbCon;
        String tmpDir;
        String duckDb;

        @Setup()
        public void setup1(PrestoExportedData prestoExportedData)
                throws SamplerImportException, SQLException, IOException
        {
            tmpDir = Files.createTempDirectory("duckdb").toFile().getAbsolutePath();
            duckDb = tmpDir + "/test_duckdb.db";
            // Import the table in a test_duckdb db
            DuckDbImporter duckDbImporter = new DuckDbImporter();
            duckDbCon = duckDbImporter.getConnection(duckDb);
            duckDbCon.setAutoCommit(false);
            duckDbImporter.importFile(duckDbCon, prestoExportedData.tmpDir, "CSV", TPCHLineitem.class.getSimpleName());
        }

        @TearDown
        public void tearDown()
                throws SQLException
        {
            duckDbCon.commit();
            duckDbCon.close();
            // FileUtils.deleteDirectory(tmpDir);
        }

        // This causes query string to be generated/invocation and the cost of generation is not included in measurements.
        @Setup(Level.Invocation)
        public void setup()
        {
            query = qg.generateRandomQuery(TPCHLineitem.class);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ResultSet likeSqliteQueries(SqliteQuery q)
            throws SQLException
    {
        PreparedStatement preparedStatement = q.sqliteCon.prepareStatement(q.query);
        return preparedStatement.executeQuery();
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public ResultSet likeDuckDbQueries(DuckDbQuery q)
            throws SQLException
    {
        PreparedStatement preparedStatement = q.duckDbCon.prepareStatement(q.query);
        return preparedStatement.executeQuery();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(".*" + JoinBenchmark.class.getSimpleName() + ".*")
                .forks(1)
                .jvmArgs("-ea")
                .shouldFailOnError(true)
                .build();

        new Runner(opt).run();
    }
}