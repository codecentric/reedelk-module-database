package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class ConversionError extends PlatformException {

    public ConversionError(String message, Exception exception) {
        super(message, exception);
    }

    public ConversionError(String message) {
        super(message);
    }
}
