package com.ibm.testbed.importer;

import com.google.common.base.Stopwatch;
import com.ibm.testbed.exporter.JdbcPrestoExporter;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
