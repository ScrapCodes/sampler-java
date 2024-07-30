package com.ibm.testbed.importer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class DuckDbImporter
        implements Importer
{
    public Connection getConnection(String dbFilePath)
            throws SamplerImportException
    {
        String url = "jdbc:duckdb:" + dbFilePath;
        try {
            return DriverManager.getConnection(url);
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while creating connection : %s", url), e);
        }
    }

    public void importFile(Connection conn, String path, String format, String tableName)
            throws SamplerImportException
    {
        try {
            String ctas = "";
            if (format.equalsIgnoreCase("parquet")) {
                ctas = "CREATE TABLE " + tableName + " AS SELECT * from read_parquet('" + path + "');";
            }
            else if (format.equalsIgnoreCase("json")) {
                ctas = "CREATE TABLE " + tableName + " AS SELECT * from read_json('" + path + "');";
            }
            else if (format.equalsIgnoreCase("csv")) {
                ctas = "CREATE TABLE " + tableName + " AS SELECT * from read_csv('" + path + "');";
            }
            PreparedStatement createTableStmt = conn.prepareStatement(ctas);
            createTableStmt.execute();
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while importing from %s into table: %s", path, tableName), e);
        }
    }
}