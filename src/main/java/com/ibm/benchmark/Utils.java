package com.ibm.benchmark;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class Utils
{
    public static <T> List<Field> getListOfStringColumns(Class<T> clazz)
    {
        Field[] declaredFields = clazz.getDeclaredFields();
        return Arrays.stream(declaredFields).filter(x -> x.getType().getName().equals("java.lang.String")).toList();
    }
}
