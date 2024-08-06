package com.ibm.benchmark;

import com.ibm.benchmark.generator.DistinctQueryGenerator;
import com.ibm.benchmark.generator.QueryGenerator;
import com.ibm.testbed.tables.TPCHLineitem;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DistinctQueryBenchmarkTest
        extends AbstractBenchmarkTest
{
    QueryGenerator queryGenerator = new DistinctQueryGenerator();

    public DistinctQueryBenchmarkTest(String dbType, String dbPath, String format)
    {
        super(dbType, dbPath, format);
    }

    @Override
    String generateQuery()
    {
        return queryGenerator.generateQuery(TPCHLineitem.class);
    }

    @Override
    String benchmarkName()
    {
        return "count_distinct_query";
    }
}
