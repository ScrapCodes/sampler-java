package com.ibm.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class JoinBenchmark
{


    @Benchmark
    public void equalityJoin()
    {

    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(JoinBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}