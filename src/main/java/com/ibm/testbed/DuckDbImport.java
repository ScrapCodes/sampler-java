package com.ibm.testbed;

import com.google.common.base.Stopwatch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DuckDbImport
{
    public Connection getConnection(String dbFilePath)
            throws SQLException
    {
        String url = "jdbc:duckdb:" + dbFilePath;
        return DriverManager.getConnection(url);
    }

    public void importFile(Connection conn, String path, String format, String tableName)
            throws SQLException
    {
        String ctas = "";
        if (format.equalsIgnoreCase("parquet")) {
            ctas = "CREATE TABLE " + tableName + " AS SELECT * from read_parquet('" + path + "');";
        } else if (format.equalsIgnoreCase("json")) {
            ctas = "CREATE TABLE " + tableName + " AS SELECT * from read_json('" + path + "');";
        }
        PreparedStatement createTableStmt = conn.prepareStatement(ctas);
        createTableStmt.execute();
    }
}