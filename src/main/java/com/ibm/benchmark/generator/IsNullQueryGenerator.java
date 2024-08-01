package com.ibm.benchmark.generator;

import java.util.List;

public class IsNullQueryGenerator
        extends AbstractQueryGenerator
        implements QueryGenerator
{
    private static final String IS_NULL_COUNT_QUERY_TEMPLATE = "SELECT count(col1) from tableName where col1 IS NULL ";
    private static final String IS_NOT_NULL_COUNT_QUERY_TEMPLATE = "SELECT count(col1) from tableName where col1 IS NOT NULL ";
    private static final List<String> NULL_CHECK_QUERY_TEMPLATES = List.of(IS_NULL_COUNT_QUERY_TEMPLATE, IS_NOT_NULL_COUNT_QUERY_TEMPLATE);

    @Override
    public <T> String generateQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, List.of(IS_NULL_COUNT_QUERY_TEMPLATE));
    }

    @Override
    public <T> String generateRandomQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, NULL_CHECK_QUERY_TEMPLATES);
    }

    @Override
    public List<String> getQueryTemplates()
    {
        return NULL_CHECK_QUERY_TEMPLATES;
    }
}
