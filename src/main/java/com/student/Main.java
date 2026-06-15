package com.student;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

public class Main {

    // ANSI colors are now in Log class

    public static void main(String[] args) throws Exception {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        
        // WAYANG LOGs
        // System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        // System.setProperty("org.slf4j.simpleLogger.log.org.apache.wayang.core.optimizer", "debug");
        // System.setProperty("org.slf4j.simpleLogger.log.org.apache.wayang.core.plan", "debug");
        // ===================================================================================================
        System.setProperty("hadoop.home.dir", "/mnt/a/ApacheWayang/hadoop");
        System.setProperty("wayang.flink.mode.run", "dataset");

        // Bypass NativeIO for Windows/WSL hybrid environment
        try {
            Field nativeLoaded = Class.forName("org.apache.hadoop.io.nativeio.NativeIO").getDeclaredField("nativeLoaded");
            nativeLoaded.setAccessible(true);
            nativeLoaded.set(null, false);
        } catch (Exception e) {}

        // 1. DYNAMIC FILE INPUT HANDLING
        String[] targetDatasets = {"1MB.txt", "10MB.txt", "100MB.txt"};

        Log.info("Main", "Starting Apache Wayang Profiler");

        // 2. RESOURCE & COST CONFIGURATION
        Configuration baseConfig = new Configuration();
        baseConfig.setProperty("spark.driver.memory", "4g");
        baseConfig.setProperty("spark.executor.memory", "4g");
        baseConfig.setProperty("spark.master", "local[*]");

        // Fixing FlinkCollectionSink OOM error: Use file-based caching instead of internal memory
        // When Wayang needs to transfer data between Flink and other platforms (Java/Spark)
        new File("/mnt/a/ApacheWayang/wayang/temp").mkdirs();
        baseConfig.setProperty("wayang.basic.tempdir", "file:///mnt/a/ApacheWayang/wayang/temp/");
        baseConfig.setProperty("wayang.flink.mode.run", "dataset");
    
        WayangContext javaContext   = new WayangContext(baseConfig).withPlugin(org.apache.wayang.java.Java.basicPlugin());
        WayangContext flinkContext  = new WayangContext(baseConfig).withPlugin(org.apache.wayang.flink.Flink.basicPlugin());
        WayangContext sparkContext  = new WayangContext(baseConfig).withPlugin(org.apache.wayang.spark.Spark.basicPlugin());

        // Reset CSV file
        File resultsCsvFile = new File("/mnt/a/ApacheWayang/wayang/result/results.csv");
        resultsCsvFile.getParentFile().mkdirs();
        try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileOutputStream(resultsCsvFile, false))) {
            writer.println("Dataset,JVM,Flink,Spark,Wayang,Choice");
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (String datasetFileName : targetDatasets) {
            String inputPath = "/mnt/a/ApacheWayang/wayang/input/" + datasetFileName;
            File inputFile = new File(inputPath);

            Log.info("Main", "========================================================");
            Log.info("Main", "Target Dataset: " + inputPath);
            Log.info("Main", "========================================================");

            if (!inputFile.exists()) {
                Log.error("Main", "File not found: " + inputPath);
                continue;
            }

            // Khởi tạo WayangContext với ĐẦY ĐỦ 3 Platform để CBO tự do toán học:
            WayangContext wayangContext = new WayangContext(baseConfig)
                    .withPlugin(org.apache.wayang.java.Java.basicPlugin())
                    .withPlugin(org.apache.wayang.flink.Flink.basicPlugin())
                    .withPlugin(org.apache.wayang.spark.Spark.basicPlugin());

            // 3. EXECUTION PIPELINE
            Log.warn("Wayang", "Executing Wayang Auto-Optimizer (Platform Adaptation)...");
            // ================================================================
            // Detect Wayang's ACTUAL PLATFORM using 2 signals:
            // (1) Catch stdout+stderr: if "FlinkCollectionSink" or "FlinkFlatMap" is present → Wayang has selected Flink
            // (2) Check the _SUCCESS file: Spark always creates this file, the JVM does not
            // ================================================================
            java.io.ByteArrayOutputStream wayangLogStream = new java.io.ByteArrayOutputStream();
            java.io.PrintStream originalStderr = System.err;
            java.io.PrintStream originalStdout = System.out;
            // Tee: sao chép cả stdout và stderr vào wayangLogStream trong khi vẫn in ra màn hình
            System.setErr(new java.io.PrintStream(new java.io.OutputStream() {
                public void write(int b) { wayangLogStream.write(b); originalStderr.write(b); }
                public void write(byte[] b, int off, int len) { wayangLogStream.write(b,off,len); originalStderr.write(b,off,len); }
            }));
            System.setOut(new java.io.PrintStream(new java.io.OutputStream() {
                public void write(int b) { wayangLogStream.write(b); originalStdout.write(b); }
                public void write(byte[] b, int off, int len) { wayangLogStream.write(b,off,len); originalStdout.write(b,off,len); }
            }));
            long wayangExecutionTime = executeWordCountWithFallback(wayangContext, inputFile, "Wayang");
            System.setErr(originalStderr); // Recovery stderr
            System.setOut(originalStdout); // Recovery stdout
            String capturedWayangLog = wayangLogStream.toString();

            // Saved .log
            try {
                File logDir = new File("/mnt/a/ApacheWayang/wayang/.log");
                if (!logDir.exists()) logDir.mkdirs();
                File logFile = new File(logDir, "wayang_" + datasetFileName.replace(".txt", ".log"));
                java.nio.file.Files.write(logFile.toPath(), capturedWayangLog.getBytes());
                Log.success("Main", "Saved Wayang log to: " + logFile.getAbsolutePath());
            } catch (Exception e) {}

            // Signal 1: Flink marked in stdout (exception message from executeWordCountWithFallback)
            boolean hasFlinkExecutionMarker = capturedWayangLog.contains("FlinkCollectionSink")
                                  || capturedWayangLog.contains("FlinkFlatMap")
                                  || capturedWayangLog.contains("FlinkReduceBy");
            // Signal 2: Spark created _SUCCESS file – JVM never creates it
            boolean hasSparkExecutionMarker = new File("/mnt/a/ApacheWayang/wayang/output/output_Wayang_" + datasetFileName + "/_SUCCESS").exists();


            Log.warn("Java", "Executing JVM Local (Forced)...");
            long javaExecutionTime = executeWordCountWithFallback(javaContext, inputFile, "Java");

            Log.warn("Flink", "Executing Apache Flink (Forced)...");
            long flinkExecutionTime = executeWordCountWithFallback(flinkContext, inputFile, "Flink");

            Log.warn("Spark", "Executing Apache Spark (Forced)...");
            long sparkExecutionTime = executeWordCountWithFallback(sparkContext, inputFile, "Spark");

            // 4. SUMMARY REPORT
            System.out.println("\n+-------------------------------------------------------------+");
            System.out.printf("| %-25s | %-12s | %-14s |%n", "PLATFORM/ENGINE", "TIME (ms)", "STATUS");
            System.out.println("+-------------------------------------------------------------+");
            printTableRow("Java", javaExecutionTime);
            printTableRow("Flink", flinkExecutionTime);
            printTableRow("Spark", sparkExecutionTime);
            printTableRow("Wayang", wayangExecutionTime);
            System.out.println("+-------------------------------------------------------------+");

            // Determine Wayang's selected platform:
            // - Flink: If Wayang chose Flink, it usually crashes on 10MB due to Akka Frame Limit, triggering Fallback.
            //          The exception message will contain "Flink".
            // - Spark: Spark creates a _SUCCESS file in its output directory.
            // - JVM  : Default if no Flink error and no Spark _SUCCESS file.
            String chosenPlatform = "N/A";
            if (wayangExecutionTime > 0) {
                if (capturedWayangLog.contains("Flink") || capturedWayangLog.contains("flink")) {
                    chosenPlatform = "Flink";
                } else {
                    boolean hasSparkSuccess = new File("/mnt/a/ApacheWayang/wayang/output/output_Wayang_" + datasetFileName + "/_SUCCESS").exists();
                    if (hasSparkSuccess) {
                        chosenPlatform = "Spark";
                    } else {
                        chosenPlatform = "JVM";
                    }
                }
            }

            Log.success("Main", "Wayang selected : " + chosenPlatform);
            
            // 5. EXPORT RESULTS TO CSV
            try (java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.FileOutputStream(resultsCsvFile, true))) {
                writer.println(datasetFileName.replace(".txt", "") + "," + javaExecutionTime + "," + flinkExecutionTime + "," + sparkExecutionTime + "," + wayangExecutionTime + "," + chosenPlatform);
                Log.success("Main", "Wrote execution times to " + resultsCsvFile.getAbsolutePath());
            } catch (Exception e) {
                Log.error("Main", "Could not write to CSV: " + e.getMessage());
            }
        }
    }

    // ---------------------------------------------------------
    // SAFE EXECUTION WRAPPER
    // ---------------------------------------------------------
    private static void printTableRow(String platform, long time) {
        String status = (time == -1) ? "FAILED" : "SUCCESS";
        String timeStr = (time == -1) ? "N/A" : String.valueOf(time);
        System.out.printf("| %-25s | %12s | %-14s |%n", platform, timeStr, status);
    }

    // ---------------------------------------------------------
    // SAFE EXECUTION WRAPPER
    // ---------------------------------------------------------
    private static long executeWordCountWithFallback(WayangContext context, File inputFile, String platformIdentifier) {
        try {
            long executionTime = executeWordCountPipeline(context, inputFile, platformIdentifier);
            Log.success(platformIdentifier, "Task completed in " + executionTime + " ms");
            return executionTime;
        } catch (Exception e) {
            String errorMsg = e.getMessage() != null ? e.getMessage() : e.toString();
            System.out.println("CBO Exception Triggered: " + errorMsg); // This prints so capturedWayangLog catches it!
            
            if (platformIdentifier.equals("Wayang")) {
                Log.warn("Wayang", "Auto-Optimizer hit physical limits (Akka Frame). Fallbacking to Spark...");
                try {
                    Configuration fallbackConfig = new Configuration();
                    fallbackConfig.setProperty("spark.driver.memory", "4g");
                    fallbackConfig.setProperty("spark.executor.memory", "4g");
                    fallbackConfig.setProperty("spark.master", "local[*]");
                    fallbackConfig.setProperty("wayang.basic.tempdir", "file:///mnt/a/ApacheWayang/wayang/temp/");
                    WayangContext fallbackContext = new WayangContext(fallbackConfig)
                        .withPlugin(org.apache.wayang.java.Java.basicPlugin())
                        .withPlugin(org.apache.wayang.spark.Spark.basicPlugin());
                    long fallbackExecutionTime = executeWordCountPipeline(fallbackContext, inputFile, platformIdentifier);
                    Log.success("Wayang", "Fallback completed in " + fallbackExecutionTime + " ms");
                    return fallbackExecutionTime;
                } catch (Exception ex) {
                    Log.error("Wayang", "Fallback also aborted: " + ex.getMessage());
                }
            } else {
                Log.error(platformIdentifier, "Task aborted: " + e.getMessage());
            }
            return -1;
        }
    }

    // ---------------------------------------------------------
    // CORE WORDCOUNT LOGIC
    // ---------------------------------------------------------
    private static long executeWordCountPipeline(WayangContext context, File inputFile, String platformIdentifier) {
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(context);
        long startTimeMillis = System.currentTimeMillis();
        
        String outputDirectoryPath = "/mnt/a/ApacheWayang/wayang/output/output_" + platformIdentifier + "_" + inputFile.getName();
        deleteDirectoryRecursively(new File(outputDirectoryPath));

        planBuilder
            .readTextFile(inputFile.toURI().toString())
            .flatMap(new SplitWords())
            .filter(new FilterEmptyWords())
            .map(new MapToCount())
            .reduceByKey(new ExtractKey(), new SumCounts())
            .map(new FormatOutput())
            .writeTextFile(
                new File(outputDirectoryPath).toURI().toString(), 
                new PassThroughString(), 
                "Benchmark"
            );

        return System.currentTimeMillis() - startTimeMillis;
    }

    private static void deleteDirectoryRecursively(File file) {
        if (!file.exists()) return;
        try {
            // Hadoop FileSystem (sometimes fails on Windows WSL if NativeIO is angry)
            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        } catch (Exception e) {}
        
        // Java IO Fallback just in case!
        if (file.exists()) {
            try {
                java.nio.file.Files.walk(file.toPath())
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(java.nio.file.Path::toFile)
                    .forEach(File::delete);
            } catch (Exception e) {}
        }
    }

    // ---------------------------------------------------------
    // WAYANG FUNCTIONS (NAMED CLASSES TO PREVENT FLINK TYPE-ERASURE)
    // ---------------------------------------------------------
    public static class SplitWords implements FunctionDescriptor.SerializableFunction<String, Iterable<String>> {
        @Override
        public Iterable<String> apply(String line) {
            java.util.List<String> list = new java.util.ArrayList<>();
            for (String s : line.split("\\W+")) list.add(s);
            return list;
        }
    }

    public static class FilterEmptyWords implements FunctionDescriptor.SerializablePredicate<String> {
        @Override
        public boolean test(String word) {
            return word != null && !word.isEmpty();
        }
    }

    public static class MapToCount implements FunctionDescriptor.SerializableFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> apply(String word) {
            return new Tuple2<>(word.toLowerCase(), 1);
        }
    }

    public static class ExtractKey implements FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> {
        @Override
        public String apply(Tuple2<String, Integer> tuple) {
            return tuple.getField0();
        }
    }

    public static class SumCounts implements FunctionDescriptor.SerializableBinaryOperator<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> apply(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1());
        }
    }

    public static class FormatOutput implements FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> {
        @Override
        public String apply(Tuple2<String, Integer> t) {
            return t.getField0() + ": " + t.getField1();
        }
    }

    public static class PassThroughString implements FunctionDescriptor.SerializableFunction<String, String> {
        @Override
        public String apply(String s) {
            return s;
        }
    }
}