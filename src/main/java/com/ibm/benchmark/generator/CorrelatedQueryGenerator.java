package com.ibm.benchmark.generator;

import java.util.List;

public class CorrelatedQueryGenerator
        extends AbstractQueryGenerator
        implements QueryGenerator
{

    @Override
    public <T> String generateQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, getQueryTemplates());
    }

    @Override
    public <T> String generateRandomQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, getQueryTemplates());
    }

    @Override
    public List<String> getQueryTemplates()
    {
        String CORRELATED_QUERY_TEMPLATE = "SELECT count(*) FROM tableName WHERE col1 = ANY (SELECT col1 FROM tableName " +
                "                       WHERE tableName.col2 = t1.column2);";
        return List.of(CORRELATED_QUERY_TEMPLATE);
    }
}
