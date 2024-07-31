package com.ibm.testbed.importer;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SqliteImporterTest
        extends AbstractImporterTest
{

    @Test
    public void testAImportFile()
            throws SQLException, SamplerImportException
    {
        SqliteImporter sqliteImporter = new SqliteImporter();
        importData(sqliteImporter, this.getDbType());
    }

    @Override
    String getDbType()
    {
        return "sqlite";
    }
}
