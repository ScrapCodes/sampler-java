package com.ibm.testbed;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

public class Utils
{

    public static String createTable(String tableName, boolean unboundedVarchar)
    {
        return "CREATE TABLE IF NOT EXISTS " + tableName + " ( " +
                " orderkey bigint,         ".trim() +
                " partkey bigint,          ".trim() +
                " suppkey bigint,          ".trim() +
                " linenumber integer,      ".trim() +
                " quantity double,         ".trim() +
                " extendedprice double,    ".trim() +
                " discount double,         ".trim() +
                " tax double,              ".trim() +
                " returnflag varchar(1),   ".trim() +
                " linestatus varchar(1),   ".trim() +
                " shipdate date,           ".trim() +
                " commitdate date,         ".trim() +
                " receiptdate date,        ".trim() +
                " shipinstruct varchar(25),".trim() +
                " shipmode varchar(10),    ".trim() +
                " comment varchar(44))     ".trim();
    }

    public static void deleteFilesByWildChar(String basePath, String wildCardValue)
    {
        final Path dir = Paths.get(basePath);

        try {
            Stream<Path> results = Files.find(dir,
                    Integer.MAX_VALUE,
                    (path, basicFileAttributes) -> path.toFile().getName().matches(wildCardValue)
            );

            Preconditions.checkArgument(results.map(p -> new File(p.toString()).delete()).reduce(true, (x, y) -> x && y), "Files failed to delete");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
