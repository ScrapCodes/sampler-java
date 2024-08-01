package com.ibm.benchmark.generator;

import com.google.common.base.Preconditions;
import com.ibm.benchmark.Utils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public abstract class AbstractQueryGenerator
        implements QueryGenerator
{

    @Override
    public <T> String generateQuery(Class<T> tableType, String col1, String col2)
    {
        return generateQuery(tableType, getQueryTemplates(), col1, col2);
    }

    /*
     * possible values of comment
     *   presto:tpch> select count(orderkey) as c1, comment from lineitem group by comment ORDER BY c1 DESC LIMIT 6;
     *     c1  |   comment
     *   ------+-------------
     *    9523 |  furiously
     *    9149 |  carefully
     *    8484 | carefully
     *    8477 |  furiously
     *    8414 |  carefully
     *    8413 | furiously
     *   (6 rows)
     */
    private final Map<String, List<String>> ColNameVsPossibleValues = Map.of(
            "returnflag", List.of("R", "A", "N"),
            "linestatus", List.of("O", "F"),
            "shipinstruct", List.of("NONE", "COLLECT COD", "DELIVER IN PERSON", "TAKE BACK RETURN"),
            "shipmode", List.of("REG AIR", "TRUCK", "MAIL", "FOB"),
            "comment", List.of("  carefully ", "  furiously ")
    );

    int getRandomInt(int minVal, int maxVal)
    {
        Preconditions.checkArgument(minVal < maxVal && minVal >= 0);
        return (int) ((Math.random() * (maxVal - minVal)) + minVal);
    }

    String getRandomSubstring(String s)
    {
        int len = s.length();
        if (len <= 1) {
            return s;
        }
        int startIndex = getRandomInt(0, len / 2);
        int endIndex = getRandomInt(len / 2, len);
        return s.substring(startIndex, endIndex);
    }

    protected <T> String generateRandomQuery(Class<T> tableType, List<String> queryTemplates)
    {
        List<Field> listOfVarcharColumns = Utils.getListOfStringColumns(tableType);
        Preconditions.checkArgument(listOfVarcharColumns.size() > 1);

        // Pick a template for generating like query.
        String col1 = listOfVarcharColumns.get(getRandomInt(0, listOfVarcharColumns.size())).getName();
        String col2 = listOfVarcharColumns.get(getRandomInt(0, listOfVarcharColumns.size())).getName();
        return generateQuery(tableType, queryTemplates, col1, col2);
    }

    protected <T> String generateQuery(Class<T> tableType, List<String> queryTemplates, String col1, String col2)
    {
        String templateQuery = queryTemplates.get(getRandomInt(0, queryTemplates.size()));
        templateQuery = templateQuery.replace("col1", col1);
        String tableName = tableType.getSimpleName();
        templateQuery = templateQuery.replace("tableName", tableName);

        // get value for equality condition.
        List<String> strings2 = ColNameVsPossibleValues.get(col2);
        templateQuery = templateQuery.replace("col2", col2)
                .replace("val2", strings2.get(getRandomInt(0, strings2.size())));

        // generate like string for the columns.
        List<String> strings1 = ColNameVsPossibleValues.get(col1);
        String likeString = strings1.get(getRandomInt(0, strings1.size())).trim();
        return String.format(templateQuery, getRandomSubstring(likeString));
    }
}
