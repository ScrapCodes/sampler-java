package com.ibm.testbed.importer;

import com.google.common.base.Stopwatch;
import com.ibm.testbed.exporter.JdbcPrestoExporter;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractImporterTest
{
    String path;

    long expectedCount;

    String dbPath;

    public AbstractImporterTest()
    {
        try {
            path = Files.createTempDirectory("presto_test_export").toFile().getAbsolutePath();
            dbPath = String.format("/tmp/test_%s_%s.db", "n", getDbType());
            exportData();
        }
        catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract String getDbType();

    protected abstract String exportFormat();

    protected String jdbcUrl()
    {
        return String.format("jdbc:%s:%s", getDbType(), dbPath);
    }

    private synchronized void exportData()
            throws SQLException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        JdbcPrestoExporter j = new JdbcPrestoExporter();
        Connection connection = j.getConnectionWithDefaults();
        expectedCount = j.exportTable(connection, path, exportFormat(), 2);
        sw.stop();
        System.out.printf("\nTime to export %d records as %s %s : %dms", expectedCount, exportFormat(), path, sw.elapsed().toMillis());
    }

    @Test
    public void importData()
            throws SQLException, SamplerImportException
    {
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.reset();
        Importer importer = ImporterFactory.createInstance(getDbType());
        String dbUrl = jdbcUrl();
        try (Connection con = importer.getConnection(jdbcUrl())) {
            con.setAutoCommit(false);
            String tableName = "TPCHLineitem";
            sw.start();
            importer.importFile(con, path, exportFormat(), tableName);
            sw.stop();
            System.out.printf("\nTime to import %d records into %s at '%s' : %dms", expectedCount, tableName, dbUrl, sw.elapsed().toMillis());
            if (Files.isReadable(Path.of(dbPath))) {
                System.out.printf("\n Size of the %s db file after importing the dataset %d bytes", getDbType(), FileUtils.sizeOf(new File(dbPath)));
            }
            PreparedStatement statement = con.prepareStatement(String.format("SELECT * FROM %s LIMIT 1", tableName)); // To verify imported table has all columns
            ResultSet resultSet = statement.executeQuery();
            assertEquals("Imported table has missing columns", 16, resultSet.getMetaData().getColumnCount());
            statement = con.prepareStatement(String.format("SELECT count(*) FROM %s ", tableName));
            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            assertEquals("mismatch in number of rows exported and number of rows imported", expectedCount, resultSet.getLong(1));
        }
    }

    @After
    public void zCleanup()
            throws IOException
    {
        // cleanup each test
        System.out.println("\nCleaning up! : " + path);
        FileUtils.deleteDirectory(new File(path));
        Files.deleteIfExists(Path.of(dbPath));
    }
}
