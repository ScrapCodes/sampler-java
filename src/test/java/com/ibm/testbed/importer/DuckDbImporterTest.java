package com.ibm.testbed.importer;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DuckDbImporterTest
        extends AbstractImporterTest
{

    @Test
    public void testAImportFile()
            throws SQLException, SamplerImportException
    {
        DuckDbImporter duckDbImporter = new DuckDbImporter();
        importData(duckDbImporter, this.getDbType());
    }

    @Override
    String getDbType()
    {
        return "duckdb";
    }
}
