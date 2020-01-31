package com.reedelk.mysql;

public class DatabaseUtils {

    public static void closeSilently(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}
