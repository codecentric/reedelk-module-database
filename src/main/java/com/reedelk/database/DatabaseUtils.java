package com.reedelk.database;

import java.util.Arrays;

public class DatabaseUtils {

    public static void closeSilently(AutoCloseable ...closeables) {
        Arrays.stream(closeables).forEach(autoCloseable -> {
            if (autoCloseable != null) {
                try {
                    autoCloseable.close();
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
        });
    }
}
