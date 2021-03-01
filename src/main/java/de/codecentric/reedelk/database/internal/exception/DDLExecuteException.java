package de.codecentric.reedelk.database.internal.exception;

import de.codecentric.reedelk.runtime.api.exception.PlatformException;

public class DDLExecuteException extends PlatformException {

    public DDLExecuteException(String message) {
        super(message);
    }

    public DDLExecuteException(String message, Throwable exception) {
        super(message, exception);
    }
}
