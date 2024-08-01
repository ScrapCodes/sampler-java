package com.ibm.benchmark.generator;

import java.util.List;

public interface QueryGenerator
{
    /**
     * Generate a query of "this" type. for example a DISTINCT count query or a join query.
     */
    <T> String generateQuery(Class<T> tableType);

    /**
     * Generate a query of a mixed type, e.g. DISTINCT count JOIN query or LIKE and EQUALITY.
     */
    <T> String generateRandomQuery(Class<T> tableType);

    /**
     * Get query template, can be used to generate queries.
     */
    List<String> getQueryTemplates();

    /**
     * Generate a query with specific columns names.
     */
    <T> String generateQuery(Class<T> tableType, String col1, String col2);
}
