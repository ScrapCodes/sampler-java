package com.ibm.testbed.importer;

import java.sql.Connection;

public interface Importer
{
    Connection getConnection(String dbFilePath)
            throws SamplerImportException;

    void importFile(Connection conn, String path, String format, String tableName)
            throws SamplerImportException;
}
