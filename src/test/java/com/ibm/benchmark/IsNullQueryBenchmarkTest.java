package com.ibm.benchmark;

import com.ibm.benchmark.generator.DistinctQueryGenerator;
import com.ibm.benchmark.generator.IsNullQueryGenerator;
import com.ibm.benchmark.generator.QueryGenerator;
import com.ibm.testbed.tables.TPCHLineitem;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IsNullQueryBenchmarkTest
        extends AbstractBenchmarkTest
{
    QueryGenerator queryGenerator = new IsNullQueryGenerator();

    public IsNullQueryBenchmarkTest(String dbType, String dbPath, String format)
    {
        super(dbType, dbPath, format);
    }

    @Override
    String generateQuery()
    {
        return queryGenerator.generateQuery(TPCHLineitem.class).replace(TPCHLineitem.class.getSimpleName(), tableName());
    }

    @Override
    String benchmarkName()
    {
        return "is_null_query";
    }
}
