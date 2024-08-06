package com.ibm.benchmark;

import com.ibm.benchmark.generator.JoinQueryGenerator;
import com.ibm.benchmark.generator.QueryGenerator;
import com.ibm.testbed.tables.TPCHLineitem;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JoinLikeQueryBenchmarkTest
        extends AbstractBenchmarkTest
{
    QueryGenerator queryGenerator = new JoinQueryGenerator();

    public JoinLikeQueryBenchmarkTest(String dbType, String dbPath, String format)
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
        return "join_like_query";
    }
}
