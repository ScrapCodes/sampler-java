package com.ibm.testbed;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.opencsv.bean.CsvBindByPosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class Utils
{
    private static final Logger logger = LogManager.getLogger();

    public static String createTable(String tableName, boolean unboundedVarchar)
    {
        // TODO autogenerate this from Bean POJO, similar to genInsertSQLString method.
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " ( " +
                " orderkey bigint,         ".trim() +
                " partkey bigint,          ".trim() +
                " suppkey bigint,          ".trim() +
                " linenumber integer,      ".trim() +
                " quantity double,         ".trim() +
                " extendedprice double,    ".trim() +
                " discount double,         ".trim() +
                " tax double,              ".trim();
        if (!unboundedVarchar) {
            return createTableSQL + " returnflag varchar(1),   ".trim() +
                    " linestatus varchar(1),   ".trim() +
                    " shipdate date,           ".trim() +
                    " commitdate date,         ".trim() +
                    " receiptdate date,        ".trim() +
                    " shipinstruct varchar(25),".trim() +
                    " shipmode varchar(10),    ".trim() +
                    " comment varchar(44))     ".trim();
        }
        else {
            return createTableSQL.replace("bigint", "varchar")
                    .replace("integer", "varchar")
                    .replace("double", "varchar") +
                    " returnflag varchar,  ".trim() +
                    " linestatus varchar,  ".trim() +
                    " shipdate varchar,    ".trim() +
                    " commitdate varchar,  ".trim() +
                    " receiptdate varchar, ".trim() +
                    " shipinstruct varchar,".trim() +
                    " shipmode varchar,    ".trim() +
                    " comment varchar)".trim();
        }
    }

    public static void deleteFilesByWildChar(String basePath, String wildCardValue)
    {
        final Path dir = Paths.get(basePath);

        try (Stream<Path> results = Files.find(dir,
                Integer.MAX_VALUE,
                (path, basicFileAttributes) -> path.toFile().getName().matches(wildCardValue)
        )) {

            Preconditions.checkArgument(results.map(p -> new File(p.toString()).delete()).reduce(true, (x, y) -> x && y), "Files failed to delete");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<String> listFilesByWildChar(String basePath, String wildCardValue)
    {
        final Path dir = Paths.get(basePath);

        try (Stream<Path> results = Files.find(dir,
                Integer.MAX_VALUE,
                (path, basicFileAttributes) -> path.toFile().getName().matches(wildCardValue)
        )) {
            return results.map(Path::toString).toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void decompressGzip(Path source, Path target)
            throws IOException
    {

        try (GZIPInputStream gis = new GZIPInputStream(
                new FileInputStream(source.toFile()))) {
            try {
                Files.copy(gis, target);
            }
            catch (FileAlreadyExistsException fe) {
                logger.warn("File already exists: {}", target, fe);
            }
        }
    }

    public static <T> String genInsertSQLString(String tableName, Class<T> clazz)
    {
        String columnNames = String.join(", ",
                Arrays.stream(clazz.getDeclaredFields()).map(Field::getName).toList());
        return String.format("INSERT INTO %s (%s) VALUES (?%s)", tableName, columnNames,
                Strings.repeat(", ?", clazz.getDeclaredFields().length - 1));
    }

    private static void setInsertField(Field f, PreparedStatement insertStatement, Object value)
            throws IllegalAccessException, SQLException
    {
        int index = f.getAnnotation(CsvBindByPosition.class).position() + 1; // jdbc index starts from 1 v/s CSVParser index starts from 0.
        logger.trace("Field name: {} pos: {} value {}}", f.getName(), index, f.get(value));
        switch (f.getType().getName()) {
            case "java.lang.Long" -> insertStatement.setLong(index, (Long) f.get(value));
            case "java.lang.Integer" -> insertStatement.setInt(index, (Integer) f.get(value));
            case "java.sql.Date" -> insertStatement.setDate(index, (Date) f.get(value));
            case "java.lang.String" -> insertStatement.setString(index, (String) f.get(value));
            case "java.lang.Double" -> insertStatement.setDouble(index, (Double) f.get(value));
            default -> throw new UnsupportedOperationException("Not supported type! " + f.getType().getName());
        }
    }

    public static <T> void writeJdbcBatch(
            Connection conn,
            String tableName,
            int batchSize,
            List<T> rows,
            Class<T> clazz)
            throws SQLException, IllegalAccessException
    {
        String insertSQL = genInsertSQLString(tableName, clazz);
        PreparedStatement insertStatement = conn.prepareStatement(insertSQL); {
            int recordCount = 0;
            for (T row : rows) {
                for (Field f : clazz.getDeclaredFields()) {
                    setInsertField(f, insertStatement, row);
                }
                insertStatement.addBatch();
                recordCount++;
                if (recordCount >= batchSize) {
                    recordCount = 0;
                    insertStatement.executeUpdate();
                }
            }
            if (recordCount > 0) {
                insertStatement.executeUpdate();
            }
        }
    }
}
