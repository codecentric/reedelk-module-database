package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class SelectException extends PlatformException {

    public SelectException(String message, Throwable exception) {
        super(message, exception);
    }
}
