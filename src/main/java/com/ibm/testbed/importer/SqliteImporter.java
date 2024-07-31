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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SqliteImporter
        implements Importer
{
    private static final Logger logger = LogManager.getLogger();

    @Override
    public Connection getConnection(String dbFilePath)
            throws SamplerImportException
    {
        String url = String.format("jdbc:sqlite:%s", dbFilePath);
        try {
            return DriverManager.getConnection(url);
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while creating connection : %s", url), e);
        }
    }

    @Override
    public void importFile(Connection conn, String path, String format, String tableName)
            throws SamplerImportException
    {
        Preconditions.checkArgument(format.equalsIgnoreCase("csv"), "Only CSV format is supported for import.");
        try {
            importCSVPath(conn, path, tableName);
        }
        catch (Exception e) {
            throw new SamplerImportException(String.format("Error while importing from %s into table: %s", path, tableName), e);
        }
    }

    private void importCSVPath(Connection conn, String path, String tableName)
            throws SQLException, IOException, IllegalAccessException
    {
        Statement createStmt = conn.createStatement();
        createStmt.execute(Utils.createTable(tableName, false));
        conn.setAutoCommit(false);
        if (Files.isDirectory(Path.of(path))) {
            List<String> files = Utils.listFilesByWildChar(path, ".*.gz");
            // presto produces gz regardless of setting hive.compression-codec to NONE at server side.
            // There is a similar issue: https://github.com/prestodb/presto/issues/7826
            // TODO do some more verification and open an issue.
            for (String f : files) {
                logger.info("decompress file:{}", f);
                String targetFile = f.replace("gz", "csv");
                Utils.decompressGzip(Path.of(f), Path.of(targetFile));
                List<TPCHLineitem> tpchLineitems = new CsvToBeanBuilder<TPCHLineitem>(new FileReader(targetFile)).withType(TPCHLineitem.class).build().parse();
                // table is divided into parts as different files and each file may fit in memory, while the table won't.
                Utils.writeJdbcBatch(conn, tableName, 10000, tpchLineitems, TPCHLineitem.class);
            }
        }
        else {
            throw new UnsupportedOperationException("Input path as file, is not supported !");
        }
        conn.commit();
    }
}
