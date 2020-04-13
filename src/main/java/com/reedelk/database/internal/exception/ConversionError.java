package com.reedelk.database.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class ConversionError extends PlatformException {

    public ConversionError(String message) {
        super(message);
    }
}
