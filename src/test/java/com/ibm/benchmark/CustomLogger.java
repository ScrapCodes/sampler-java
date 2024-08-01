package com.ibm.benchmark;

public class CustomLogger
{

    public void info(String message, Object p0, Object p1)
    {
        System.err.printf("\n" + message.replaceFirst("\\{}", p0.toString()).replaceFirst("\\{}", p1.toString()));
    }

    public void info(String message, Object p0, Object p1, Object p2)
    {
        System.err.printf("\n" + message.replaceFirst("\\{}", p0.toString())
                .replaceFirst("\\{}", p1.toString()).replaceFirst("\\{}", p2.toString()));
    }

    public void info(String message, Object p0, Object p1, Object p2, Object p3)
    {
        System.err.printf("\n" + message.replaceFirst("\\{}", p0.toString())
                .replaceFirst("\\{}", p1.toString()).replaceFirst("\\{}", p2.toString())
                .replaceFirst("\\{}", p3.toString()));
    }
}
