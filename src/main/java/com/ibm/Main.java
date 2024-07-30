package com.ibm;

import com.google.common.base.Stopwatch;
import com.ibm.testbed.exporter.JdbcPrestoExporter;
import com.ibm.testbed.importer.SQLiteImporter;
import com.ibm.testbed.importer.SamplerImportException;

import java.sql.Connection;
import java.sql.SQLException;

public class Main
{
    public static void main(String[] args)
            throws SQLException, SamplerImportException
    {
        JdbcPrestoExporter j = new JdbcPrestoExporter();
        Connection connection = j.getConnectionWithDefaults();
        // j.exportTable(connection, "/drive1/temp/", "CSV");
        // Exception in thread "main" java.sql.SQLException: Query failed (#20240726_172345_00001_k4hku):
        // Hive CSV storage format only supports VARCHAR (unbounded).
        // Unsupported columns: orderkey bigint, partkey bigint, suppkey bigint, linenumber integer, quantity double, extendedprice double, discount double, tax double,
        // returnflag varchar(1), linestatus varchar(1), shipdate date, commitdate date, receiptdate date, shipinstruct varchar(25), shipmode varchar(10), comment varchar(44)
        Stopwatch sw = Stopwatch.createUnstarted();
        sw.start();
        String basePath = "/drive1/temp3/";
        //j.exportTable(connection, basePath, "CSV");
        sw.stop();
        System.out.println("Elapsed time to export: " + sw.elapsed().toSeconds());
        sw.reset();
//        Utils.deleteFilesByWildChar(basePath, ".*.crc");
//        DuckDbImport dbImport = new DuckDbImport();
//        Connection duckdbConn = dbImport.getConnection("/drive1/test3.db");
//        sw.start();
//        dbImport.importFile(duckdbConn, "/drive1/temp/*", "PARQUET", "lineitem");
//        sw.stop();
//        System.out.println("Elapsed time: " + sw.elapsed().toSeconds());
//        sw.start();
//        dbImport.importFile(duckdbConn, basePath + "*", "CSV", "lineitem");
//        sw.stop();
        SQLiteImporter sqLiteImporter = new SQLiteImporter();
        Connection sqliteCon = sqLiteImporter.getConnection("/drive1/sqlite-test.db");
        sw.start();
        sqLiteImporter.importFile(sqliteCon, basePath, "csv", "lineitem");
        sw.stop();
        System.out.println("Elapsed time: " + sw.elapsed().toSeconds());
    }
}
