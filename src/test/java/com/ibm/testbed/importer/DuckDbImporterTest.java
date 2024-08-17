package com.ibm.testbed.importer;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DuckDbImporterTest
        extends AbstractImporterTest
{
    @Override
    protected String getDbType()
    {
        return "duckdb";
    }

    @Override
    protected String exportFormat()
    {
        return "CSV";
    }
}
