package com.ibm.testbed.importer;

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
        try {
            if (Files.isDirectory(Path.of(path))) {
                path = path + "/*.gz";
            }
            conn.setAutoCommit(false);
            Statement createStmt = conn.createStatement();
            createStmt.execute(Utils.createTable(tableName, false));
            String insertSelect = "";
            if (format.equalsIgnoreCase("parquet")) {
                insertSelect = "INSERT INTO " + tableName + " SELECT * from read_parquet('" + path + "');";
            }
            else if (format.equalsIgnoreCase("json")) {
                insertSelect = "INSERT INTO " + tableName + " SELECT * from read_json('" + path + "');";
            }
            if (format.equalsIgnoreCase("csv")) {
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