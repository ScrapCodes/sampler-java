package com.ibm.testbed.importer;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.SQLException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SqliteImporterTest
        extends AbstractImporterTest
{

    @Override
    protected String getDbType()
    {
        return "sqlite";
    }

    @Override
    protected String exportFormat()
    {
        return "CSV";
    }
}
