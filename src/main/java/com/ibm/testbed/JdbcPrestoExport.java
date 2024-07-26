package com.ibm.testbed;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import static com.ibm.testbed.Utils.createTable;

public class JdbcPrestoExport
{
    public Connection getConnectionWithDefaults()
            throws SQLException
    {
        String url = "jdbc:presto://localhost:8080/hive/tpch";
        Properties properties = new Properties();

        properties.setProperty("user", "presto");
        properties.setProperty("password", "");
        properties.setProperty("SSL", "false");
        return DriverManager.getConnection(url, properties);
    }

    public void exportTable(Connection conn, String path, String format)
            throws SQLException
    {
        // Presto cannot set "external_location" property during a CTAS,
        // So as a workaround, first create the table and then Insert-Select
        String tableName = "lineitem_sample_" + format;
        String createTableSQL =  createTable(tableName) +" WITH ( " +
                "     format = '" + format + "',\n" +
                "     external_location = 'file://" + path + "'" +
                " )";
        String insertSelect = "INSERT INTO " + tableName + " SELECT * FROM lineitem TABLESAMPLE BERNOULLI(90)";
        PreparedStatement createTableStmt = conn.prepareStatement(createTableSQL);
        createTableStmt.execute();
        PreparedStatement insertStmt = conn.prepareStatement(insertSelect);
        insertStmt.execute();
    }
}
