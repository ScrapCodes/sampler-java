package com.ibm.benchmark;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JoinQueryGenerator
{
    private static final String LIKE_QUERY_TEMPLATE1 = "SELECT count(col1) from tableName where col1 ILIKE '%%%s%%' ";
    private static final String LIKE_QUERY_TEMPLATE2 = "SELECT count(col1) from tableName where col1 ILIKE '%s%%' ";
    private static final String LIKE_QUERY_TEMPLATE3 = "SELECT count(col1) from tableName where col1 ILIKE '%%%s' ";
    private static final String LIKE_EQUALITY_TEMPLATE = "SELECT count(col1) from tableName where col1 ILIKE '%%%s' AND col2 = 'val2' ";
    private static final List<String> LIKE_QUERY_TEMPLATES = List.of(LIKE_QUERY_TEMPLATE1, LIKE_QUERY_TEMPLATE2,
            LIKE_QUERY_TEMPLATE3, LIKE_EQUALITY_TEMPLATE);

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
    private static final Map<String, List<String>> ColNameVsPossibleValues = Map.of(
            "returnflag", List.of("R", "A", "N"),
            "linestatus", List.of("O", "F"),
            "shipinstruct", List.of("NONE", "COLLECT COD", "DELIVER IN PERSON", "TAKE BACK RETURN"),
            "shipmode", List.of("REG AIR", "TRUCK", "MAIL", "FOB"),
            "comment", List.of("  carefully ", "  furiously ")
    );

    static int getRandomInt(int minVal, int maxVal)
    {
        Preconditions.checkArgument(minVal < maxVal && minVal >= 0);

        return (int) ((Math.random() * (maxVal - minVal)) + minVal);
    }

    static String getRandomSubstring(String s)
    {
        int len = s.length();
        if (len <= 1) {
            return s;
        }
        int startIndex = getRandomInt(0, len / 2);
        int endIndex = getRandomInt(len / 2, len);

        return s.substring(startIndex, endIndex);
    }

    public static <T> String generateRandomLikeQuery(Class<T> tableType)
    {
        List<Field> listOfVarcharColumns = Utils.getListOfStringColumns(tableType);
        Collections.shuffle(listOfVarcharColumns);
        Preconditions.checkArgument(listOfVarcharColumns.size() > 1);
        // Pick a template for generating like query.
        String col1 = listOfVarcharColumns.getFirst().getName();
        String col2 = listOfVarcharColumns.get(1).getName();
        String templateQuery = LIKE_QUERY_TEMPLATES.get((int) (Math.floor(Math.random() * 10) % LIKE_QUERY_TEMPLATES.size()));
        templateQuery = templateQuery.replace("col1", col1);
        String tableName = Arrays.stream(tableType.getName().split("\\.")).toList().getLast();
        templateQuery = templateQuery.replace("tableName", tableName);

        // get value for equality condition.
        List<String> strings2 = ColNameVsPossibleValues.get(col2);
        templateQuery = templateQuery.replace("col2", col2).replace("val2", strings2.get(getRandomInt(0, strings2.size())));

        // generate like string for the columns.
        List<String> strings1 = ColNameVsPossibleValues.get(col1);
        String likeString = strings1.get(getRandomInt(0, strings1.size())).trim();
        return String.format(templateQuery, likeString);
    }
}
