package com.ibm.testbed.importer;

public class PrestoDbImporterTest
        extends AbstractImporterTest
{

    @Override
    protected String getDbType()
    {
        return "prestodb";
    }

    @Override
    protected String exportFormat()
    {
        return "PARQUET";
    }

    @Override
    protected String jdbcUrl()
    {
        return "jdbc:presto://localhost:8080/hive/tpch";
    }
}
