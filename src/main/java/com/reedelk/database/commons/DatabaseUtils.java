package com.reedelk.database.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseUtils {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseUtils.class);

    public static void closeSilently(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable exception) {
                String message = String.format("Could not close: %s", exception.getMessage());
                logger.warn(message, exception);
            }
        }
    }
}
