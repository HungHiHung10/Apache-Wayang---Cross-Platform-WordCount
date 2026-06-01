package com.student;

public class Log {

    private static final String RESET  = "\u001B[0m";
    private static final String GREEN  = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String RED    = "\u001B[31m";
    private static final String CYAN   = "\u001B[36m";

    private static String now() {
        return java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern(
                        "yyyy-MM-dd HH:mm:ss"));
    }

    public static void info(String module, String msg) {
        System.out.printf(
            "[%s] %s[INFO ] [%s] %s%s%n",
            now(), CYAN, pad(module), msg, RESET
        );
    }

    public static void warn(String module, String msg) {
        System.out.printf(
            "[%s] %s[WARN ] [%s] %s%s%n",
            now(), YELLOW, pad(module), msg, RESET
        );
    }

    public static void error(String module, String msg) {
        System.out.printf(
            "[%s] %s[ERROR] [%s] %s%s%n",
            now(), RED, pad(module), msg, RESET
        );
    }

    public static void success(String module, String msg) {
        System.out.printf(
            "[%s] %s[SUCCESS] [%s] %s%s%n",
            now(), GREEN, pad(module), msg, RESET
        );
    }

    private static String pad(String s) {
        return String.format("%-12s", s);
    }
}
