package com.ibm.testbed.importer;

/**
 * Given a jdbc URL get an instance of the Importer of appropriate type.
 */
public class ImporterFactory
{
    public static Importer createInstance(String dbType)
    { // TODO: infer via jdbc url.
        if (dbType.equalsIgnoreCase("prestodb")) {
            return new PrestoImporter();
        }
        else if (dbType.equalsIgnoreCase("sqlite")) {
            return new SqliteImporter();
        }
        else if (dbType.equalsIgnoreCase("duckdb")) {
            return new DuckDbImporter();
        }
        else {
            return new JdbcBasedImporter();
        }
    }
}
