package de.codecentric.reedelk.database.internal.exception;

import de.codecentric.reedelk.runtime.api.exception.PlatformException;

public class DeleteException extends PlatformException {

    public DeleteException(String message, Throwable exception) {
        super(message, exception);
    }
}
