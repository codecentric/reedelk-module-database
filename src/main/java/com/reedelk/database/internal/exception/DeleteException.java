package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class DeleteException extends PlatformException {

    public DeleteException(String message, Throwable exception) {
        super(message, exception);
    }
}
