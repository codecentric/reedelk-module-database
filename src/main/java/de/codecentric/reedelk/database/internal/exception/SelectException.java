package de.codecentric.reedelk.database.internal.exception;

import de.codecentric.reedelk.runtime.api.exception.PlatformException;

public class SelectException extends PlatformException {

    public SelectException(String message, Throwable exception) {
        super(message, exception);
    }
}
