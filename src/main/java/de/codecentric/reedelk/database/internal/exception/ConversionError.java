package de.codecentric.reedelk.database.internal.exception;

import de.codecentric.reedelk.runtime.api.exception.PlatformException;

public class ConversionError extends PlatformException {

    public ConversionError(String message, Exception exception) {
        super(message, exception);
    }

    public ConversionError(String message) {
        super(message);
    }
}
