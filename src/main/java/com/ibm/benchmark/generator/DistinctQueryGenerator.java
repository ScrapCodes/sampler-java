package com.ibm.benchmark.generator;

import java.util.List;

public class DistinctQueryGenerator
        extends AbstractQueryGenerator
        implements QueryGenerator
{
    private final String COUNT_DISTINCT_QUERY_TEMPLATE = "SELECT count(col1) FROM ( SELECT DISTINCT col1 FROM tableName )";
    // following are used for random query suite only.
    private final String COUNT_DISTINCT_JOIN_QUERY_TEMPLATE = "SELECT count(col1) FROM (SELECT DISTINCT col1 from tableName where col1 LIKE '%%%s%%' )";
    private final String COUNT_DISTINCT_JOIN_EQ_QUERY_TEMPLATE = "SELECT count(col1) FROM (SELECT DISTINCT col1 from tableName where col1 LIKE '%%%s%%' AND col2 = 'val2' )";

    private final List<String> COUNT_DISTINCT_QUERY_TEMPLATES = List.of(COUNT_DISTINCT_QUERY_TEMPLATE,
            COUNT_DISTINCT_JOIN_QUERY_TEMPLATE, COUNT_DISTINCT_JOIN_EQ_QUERY_TEMPLATE);

    @Override
    public <T> String generateQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, List.of(COUNT_DISTINCT_QUERY_TEMPLATE));
    }

    @Override
    public <T> String generateRandomQuery(Class<T> tableType)
    {
        return generateRandomQuery(tableType, COUNT_DISTINCT_QUERY_TEMPLATES);
    }

    @Override
    public List<String> getQueryTemplates()
    {
        return List.of(COUNT_DISTINCT_QUERY_TEMPLATE);
    }
}
