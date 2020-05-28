package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class UpdateException extends PlatformException {

    public UpdateException(String message, Throwable exception) {
        super(message, exception);
    }
}
