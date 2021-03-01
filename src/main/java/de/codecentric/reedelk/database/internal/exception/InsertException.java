package de.codecentric.reedelk.database.internal.exception;

import de.codecentric.reedelk.runtime.api.exception.PlatformException;

public class InsertException extends PlatformException {

    public InsertException(String message, Throwable exception) {
        super(message, exception);
    }
}
