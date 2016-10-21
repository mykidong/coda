package io.shunters.coda.benchmark;

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;

import static org.junit.Assert.assertNull;


public class CodaQueueBenchmark {

    protected static final int DEFAULT_WARMUP_ITERATIONS = 10;
    protected static final int DEFAULT_MEASURE_ITERATIONS = 10;

    private static final String[] EMPTY_JVM_ARGS = {};

    private static final String[] BASE_JVM_ARGS = {
            "-server",
            "-dsa",
            "-da",
            "-ea:io.shunters.coda...",
            "-XX:+AggressiveOpts",
            "-XX:+UseBiasedLocking",
            "-XX:+UseFastAccessorMethods",
            "-XX:+OptimizeStringConcat",
            "-XX:+HeapDumpOnOutOfMemoryError"
    };

    // *************************************************************************
    //
    // *************************************************************************

    private ChainedOptionsBuilder newOptionsBuilder() {
        String className = getClass().getSimpleName();

        final ChainedOptionsBuilder runnerOptions = new OptionsBuilder()
                .include(".*" + className + ".*")
                .jvmArgs(BASE_JVM_ARGS)
                .jvmArgsAppend(jvmArgs()
                );

        if (getWarmupIterations() > 0) {
            runnerOptions.warmupIterations(getWarmupIterations());
        }

        if (getMeasureIterations() > 0) {
            runnerOptions.measurementIterations(getMeasureIterations());
        }

        if (null != getReportDir()) {
            String filePath = getReportDir() + className + ".json";
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            } else {
                file.getParentFile().mkdirs();
//                file.createNewFile();
            }

            runnerOptions.resultFormat(ResultFormatType.JSON);
            runnerOptions.result(filePath);
        }

        return runnerOptions;
    }

    private String[] jvmArgs() {
        return EMPTY_JVM_ARGS;
    }

    private int getWarmupIterations() {
        return Integer.getInteger("warmupIterations", -1);
    }

    private int getMeasureIterations() {
        return Integer.getInteger("measureIterations", -1);
    }

    private String getReportDir() {
        return System.getProperty("perfReportDir");
    }

    public static void handleUnexpectedException(Throwable t) {
        assertNull(t);
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new CodaQueueBenchmark().newOptionsBuilder().build()).run();
    }
}
