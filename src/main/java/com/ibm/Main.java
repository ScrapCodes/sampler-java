package com.ibm;

import com.google.common.base.Stopwatch;
import com.ibm.testbed.DuckDbImport;
import com.ibm.testbed.JdbcPrestoExport;
import com.ibm.testbed.Utils;

import java.sql.Connection;
import java.sql.SQLException;

public class Main
{
    public static void main(String[] args)
            throws SQLException
    {
        JdbcPrestoExport j = new JdbcPrestoExport();
        Connection connection = j.getConnectionWithDefaults();
        // j.exportTable(connection, "/drive1/temp/", "CSV");
        // Exception in thread "main" java.sql.SQLException: Query failed (#20240726_172345_00001_k4hku):
        // Hive CSV storage format only supports VARCHAR (unbounded).
        // Unsupported columns: orderkey bigint, partkey bigint, suppkey bigint, linenumber integer, quantity double, extendedprice double, discount double, tax double,
        // returnflag varchar(1), linestatus varchar(1), shipdate date, commitdate date, receiptdate date, shipinstruct varchar(25), shipmode varchar(10), comment varchar(44)
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        String basePath = "/drive1/temp2/";
        j.exportTable(connection, basePath, "JSON");
        sw.stop();
        System.out.println("Elapsed time to export: " + sw.elapsed().toSeconds());
        sw.reset();
        Utils.deleteFilesByWildChar(basePath, ".*.crc");
        DuckDbImport dbImport = new DuckDbImport();
        Connection duckdbConn = dbImport.getConnection("/drive1/test2.db");
//        sw.start();
//        dbImport.importFile(duckdbConn, "/drive1/temp/*", "PARQUET", "lineitem");
//        sw.stop();
//        System.out.println("Elapsed time: " + sw.elapsed().toSeconds());
        sw.start();
        dbImport.importFile(duckdbConn, basePath + "*", "JSON", "lineitem2");
        sw.stop();
        System.out.println("Elapsed time: " + sw.elapsed().toSeconds());
    }
}
