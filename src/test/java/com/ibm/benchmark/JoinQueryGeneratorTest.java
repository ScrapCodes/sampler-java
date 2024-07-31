package com.ibm.benchmark;

import com.ibm.testbed.tables.TPCHLineitem;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JoinQueryGeneratorTest
{
    @Test
    public void randomNumberGeneratorTest()
    {
        System.out.println(TPCHLineitem.class.getSimpleName());
        for (int i : IntStream.range(1, 20000).toArray()) {
            int randomInt = JoinQueryGenerator.getRandomInt(10, 20);
            assertTrue(randomInt < 20 && randomInt >= 10);
            int randomInt2 = JoinQueryGenerator.getRandomInt(0, 20);
            assertTrue(randomInt2 < 20 && randomInt2 >= 0);
            int randomInt3 = JoinQueryGenerator.getRandomInt(19, 20);
            assertEquals(19, randomInt3);
        }
    }

    @Test
    public void substringGeneratorTest()
    {
        for (int i : IntStream.range(1, 400000).toArray()) {
            String inputString = "check argument final";
            String str = JoinQueryGenerator.getRandomSubstring(inputString);
            assertTrue(!str.isEmpty() && str.length() < inputString.length());
        }
    }

    @Test
    public void randomLikeQueriesGeneratorTest()
    {
        for (int i : IntStream.range(1, 400).toArray()) {
            String str = JoinQueryGenerator.generateRandomLikeQuery(TPCHLineitem.class);
            System.out.println(str);  // TODO add check for query validity using sqlite.
        }
    }
}
