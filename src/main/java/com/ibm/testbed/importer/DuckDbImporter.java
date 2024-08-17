package com.ibm.testbed.importer;

import com.google.common.base.Preconditions;
import com.ibm.testbed.Utils;
import com.ibm.testbed.tables.TPCHLineitem;
import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DuckDbImporter
        extends JdbcBasedImporter
        implements Importer
{
    private static final Logger logger = LogManager.getLogger();

    @Override
    public void importFile(Connection conn, String path, String format, String tableName)
            throws SamplerImportException
    {
        Preconditions.checkArgument(Files.isDirectory(Path.of(path)), "Path must be dir");
        try {
            conn.setAutoCommit(false);
            Statement createStmt = conn.createStatement();
            createStmt.execute(Utils.createTable(tableName, false));
            String insertSelect = "";
            if (format.equalsIgnoreCase("parquet")) {
                Utils.deleteFilesByWildChar(path, ".*.crc");
                insertSelect = "INSERT INTO " + tableName + " SELECT * from read_parquet('" + path + "/*');";
            }
            else if (format.equalsIgnoreCase("json")) {
                path = path + "/*.gz";
                insertSelect = "INSERT INTO " + tableName + " SELECT * from read_json('" + path + "');";
            }
            if (format.equalsIgnoreCase("csv")) {
                path = path + "/*.gz";
                insertSelect = "INSERT INTO " + tableName + " SELECT * FROM read_csv('" + path + "', header = false);";
            }
            PreparedStatement insertStmt = conn.prepareStatement(insertSelect);
            insertStmt.execute();
            conn.commit();
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while importing from %s into table: %s", path, tableName), e);
        }
    }
}