package com.ibm.testbed;

import com.ibm.testbed.tables.TPCHLineitem;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UtilsTest
{
    @Test
    public void insertSQLGeneratorTest()
    {
        String insertSQLString = Utils.genInsertSQLString("tableName", TPCHLineitem.class);
        String expected = "INSERT INTO tableName (orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate," +
                " receiptdate, shipinstruct, shipmode, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        assertEquals(insertSQLString, expected);
    }

    @Test
    public void writeJdbcBatchTest()
            throws SQLException, IllegalAccessException
    {
        TPCHLineitem tpchLineitem = new TPCHLineitem(2L, 21L, 3L, 2, 1.0, 2.0, 0.0, 0.2, "f", "e",
                new Date(0L), new Date(0L), new Date(0L), "ins", "new", "hi");
        try (Connection sqliteConnection = DriverManager.getConnection(String.format("jdbc:sqlite:/tmp/test_%f.db", Math.random()))) {
            sqliteConnection.setAutoCommit(false);
            String tableName = "test";
            Statement createStmt = sqliteConnection.createStatement();
            createStmt.execute(Utils.createTable(tableName, false));
            List<TPCHLineitem> recordsInserted = Arrays.asList(tpchLineitem, tpchLineitem, tpchLineitem, tpchLineitem);
            Utils.writeJdbcBatch(sqliteConnection, tableName, 4, recordsInserted, TPCHLineitem.class);
            sqliteConnection.commit();
            PreparedStatement statement = sqliteConnection.prepareStatement(String.format("SELECT * FROM %s", tableName));
            ResultSet resultSet = statement.executeQuery();
            assertEquals(resultSet.getMetaData().getColumnCount(), 16);
            int recordCount = 0;
            while (resultSet.next()) {
                assertEquals(resultSet.getLong(1), 2);
                assertEquals(resultSet.getLong(2), 21);
                recordCount++;
            }
            assertEquals(recordCount, recordsInserted.size());
        }
    }
}
