package com.ibm.testbed.importer;

import com.ibm.config.Config;
import org.checkerframework.checker.units.qual.C;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

import static com.ibm.testbed.Utils.createTable;

/**
 * Import in presto is basically via table with location.
 */
public class PrestoImporter
        implements Importer
{

    @Override
    public Connection getConnection(String jdbcUrl)
            throws SamplerImportException
    {
        try {
            Config config = new Config();
            config.loadViaJavaProperties();
            Properties properties = new Properties();
            properties.setProperty("user", config.getConfWithDefault("importer.prestodb.access.user", "presto"));
            properties.setProperty("password", config.getConfWithDefault("importer.prestodb.access.password", ""));
            properties.setProperty("SSL", config.getConfWithDefault("importer.prestodb.access.ssl", "false"));
            return DriverManager.getConnection(jdbcUrl, properties);
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while creating connection : %s", jdbcUrl), e);
        }
    }

    @Override
    public void importFile(Connection conn, String path, String format, String tableName)
            throws SamplerImportException
    {
        try {

            Files.createDirectories(Paths.get(path));

            String createTableSQL = createTable(tableName, format.equalsIgnoreCase("csv")) + " WITH ( " +
                    "     format = '" + format + "',\n" +
                    "     external_location = 'file://" + path + "'" +
                    " )";
            System.out.println(createTableSQL);
            Statement createStmt = conn.createStatement();
            createStmt.execute(createTableSQL);
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while importing from %s into table: %s using PrestoImporter", path, tableName), e);
        }
    }
}
