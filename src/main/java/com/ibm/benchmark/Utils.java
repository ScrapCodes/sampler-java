package com.ibm.benchmark;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Utils
{
    public static <T> List<Field> getListOfStringColumns(Class<T> clazz)
    {
        Field[] declaredFields = clazz.getDeclaredFields();
        return Arrays.stream(declaredFields).filter(x -> x.getType().getName().equals("java.lang.String")).toList();
    }

}
