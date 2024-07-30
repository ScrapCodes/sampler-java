package com.ibm.testbed.importer;

import java.sql.Connection;

/**
 * A generic jdbc importer, given the db url and jdbc driver file on path.
 * We should be able to load the sample data inside database using jdbc.
 */
public class JdbcBasedImporter
        implements Importer
{

    @Override
    public Connection getConnection(String url)
            throws SamplerImportException
    {
        throw new SamplerImportException("Not implemented !");
    }

    @Override
    public void importFile(Connection conn, String path, String format, String tableName)
            throws SamplerImportException
    {
        throw new SamplerImportException("Not Implemented !");
    }
}
