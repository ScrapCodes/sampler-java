package com.ibm.benchmark.generator;

import java.util.List;

public class JoinQueryGenerator
        extends AbstractQueryGenerator
        implements QueryGenerator
{
    private final String LIKE_COUNT_QUERY_TEMPLATE = "SELECT count(col1) from tableName where col1 LIKE '%%%s%%' ";
    private final String LIKE_COUNT_EQUALITY_TEMPLATE = "SELECT count(col1) from tableName where col1 LIKE '%%%s%%' AND col2 = 'val2' ";
    private final List<String> LIKE_COUNT_QUERY_TEMPLATES = List.of(LIKE_COUNT_QUERY_TEMPLATE, LIKE_COUNT_EQUALITY_TEMPLATE);

    @Override
    public <T> String generateQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, List.of(LIKE_COUNT_QUERY_TEMPLATE));
    }

    @Override
    public <T> String generateRandomQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, LIKE_COUNT_QUERY_TEMPLATES);
    }

    @Override
    public List<String> getQueryTemplates()
    {
        return LIKE_COUNT_QUERY_TEMPLATES;
    }
}
