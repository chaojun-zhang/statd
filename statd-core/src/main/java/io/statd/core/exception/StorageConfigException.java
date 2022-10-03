package io.statd.core.exception;

public class StorageConfigException extends RuntimeException {

    public StorageConfigException(String message) {
        super(message);
    }

    public StorageConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageConfigException(Throwable cause) {
        super(cause);
    }
}
