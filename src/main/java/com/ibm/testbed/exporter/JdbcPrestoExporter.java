package com.ibm.testbed.exporter;

import com.ibm.config.Config;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

import static com.ibm.testbed.Utils.createTable;

public class JdbcPrestoExporter
{
    public Connection getConnectionWithDefaults()
            throws SQLException, IOException
    {
        Config config = new Config();
        config.loadViaJavaProperties();
        String url = config.getRequiredConf("exporter.prestodb.jdbc_url");
        Properties properties = new Properties();

        properties.setProperty("user", config.getConfWithDefault("exporter.prestodb.access.user", "presto"));
        properties.setProperty("password", config.getConfWithDefault("exporter.prestodb.access.password", ""));
        properties.setProperty("SSL", config.getConfWithDefault("exporter.prestodb.access.ssl", "false"));
        return DriverManager.getConnection(url, properties);
    }

    public long exportTable(Connection conn, String path, String format, int samplePercent)
            throws SQLException
    {
        // Presto cannot set "external_location" property during a CTAS,
        // So as a workaround, first create the table and then Insert-Select
        String tableName = "lineitem_sample_" + format + UUID.randomUUID().toString().substring(0, 4);
        String createTableSQL = createTable(tableName, format.equalsIgnoreCase("csv")) + " WITH ( " +
                "     format = '" + format + "',\n" +
                "     external_location = 'file://" + path + "'" +
                " )";
        String insertSelect = "INSERT INTO " + tableName + " SELECT * FROM lineitem TABLESAMPLE BERNOULLI(" + samplePercent + ")";
        // Hive CSV storage format only supports VARCHAR (unbounded).
        String insertSelectCsv = "INSERT INTO " + tableName +
                " SELECT CAST(orderkey as varchar)," +
                "CAST(partkey as varchar)," +
                "CAST(suppkey as varchar)," +
                "CAST(linenumber as varchar)," +
                "CAST(quantity as varchar)," +
                "CAST(extendedprice as varchar)," +
                "CAST(discount as varchar)," +
                "CAST(tax as varchar)," +
                "CAST(returnflag as varchar)," +
                "CAST(linestatus as varchar)," +
                "CAST(shipdate as varchar)," +
                "CAST(commitdate as varchar)," +
                "CAST(receiptdate as varchar)," +
                "CAST(shipinstruct as varchar)," +
                "CAST(shipmode as varchar)," +
                "CAST(comment as varchar)  FROM lineitem TABLESAMPLE BERNOULLI(" + samplePercent + ")";

        PreparedStatement createTableStmt = conn.prepareStatement(createTableSQL);
        createTableStmt.execute();
        if (format.equalsIgnoreCase("csv")) {
            insertSelect = insertSelectCsv;
        }
        PreparedStatement insertStmt = conn.prepareStatement(insertSelect);
        insertStmt.execute();
        PreparedStatement countQuery = conn.prepareStatement("SELECT count(*) FROM " + tableName);
        ResultSet resultSet = countQuery.executeQuery();
        if (resultSet.next()) {
            return resultSet.getLong(1);
        }
        return -1;
    }
}
