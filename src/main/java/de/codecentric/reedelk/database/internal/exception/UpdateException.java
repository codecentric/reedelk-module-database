package de.codecentric.reedelk.database.internal.exception;

import de.codecentric.reedelk.runtime.api.exception.PlatformException;

public class UpdateException extends PlatformException {

    public UpdateException(String message, Throwable exception) {
        super(message, exception);
    }
}
