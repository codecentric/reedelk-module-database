package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class InsertException extends PlatformException {

    public InsertException(String message, Throwable exception) {
        super(message, exception);
    }
}
