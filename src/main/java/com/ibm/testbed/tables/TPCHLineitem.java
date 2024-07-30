package com.ibm.testbed.tables;

import com.opencsv.bean.CsvBindByPosition;
import com.opencsv.bean.CsvDate;

import java.sql.Date;

public class TPCHLineitem
{
    @CsvBindByPosition(position = 0) public Long orderkey;
    @CsvBindByPosition(position = 1) public Long partkey;
    @CsvBindByPosition(position = 2) public Long suppkey;
    @CsvBindByPosition(position = 3) public Integer linenumber;
    @CsvBindByPosition(position = 4) public Double quantity;
    @CsvBindByPosition(position = 5) public Double extendedprice;
    @CsvBindByPosition(position = 6) public Double discount;
    @CsvBindByPosition(position = 7) public Double tax;
    @CsvBindByPosition(position = 8) public String returnflag;
    @CsvBindByPosition(position = 9) public String linestatus;
    @CsvBindByPosition(position = 10) @CsvDate("yyyy-mm-dd") public Date shipdate;
    @CsvBindByPosition(position = 11) @CsvDate("yyyy-mm-dd") public Date commitdate;
    @CsvBindByPosition(position = 12) @CsvDate("yyyy-mm-dd") public Date receiptdate;
    @CsvBindByPosition(position = 13) public String shipinstruct;
    @CsvBindByPosition(position = 14) public String shipmode;
    @CsvBindByPosition(position = 15) public String comment;

    public TPCHLineitem()
    {
        // default constructor required by CSV Reader.
    }
    // used by tests
    public TPCHLineitem(Long orderkey, Long partkey, Long suppkey, Integer linenumber, Double quantity, Double extendedprice, Double discount, Double tax, String returnflag,
            String linestatus, Date shipdate, Date commitdate, Date receiptdate, String shipinstruct, String shipmode, String comment)
    {
        this.orderkey = orderkey;
        this.partkey = partkey;
        this.suppkey = suppkey;
        this.linenumber = linenumber;
        this.quantity = quantity;
        this.extendedprice = extendedprice;
        this.discount = discount;
        this.tax = tax;
        this.returnflag = returnflag;
        this.linestatus = linestatus;
        this.shipdate = shipdate;
        this.commitdate = commitdate;
        this.receiptdate = receiptdate;
        this.shipinstruct = shipinstruct;
        this.shipmode = shipmode;
        this.comment = comment;
    }
}
