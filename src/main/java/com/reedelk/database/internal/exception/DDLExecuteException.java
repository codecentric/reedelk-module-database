package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class DDLExecuteException extends PlatformException {

    public DDLExecuteException(String message) {
        super(message);
    }

    public DDLExecuteException(String message, Throwable exception) {
        super(message, exception);
    }
}
