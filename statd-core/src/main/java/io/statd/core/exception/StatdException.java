package io.statd.core.exception;

public class StatdException extends RuntimeException {

    private ErrorCode errorCode;

    public StatdException(ErrorCodeSupplier errorCode, String message)
    {
        this(errorCode, message, null);
    }

    public StatdException(ErrorCodeSupplier errorCode, Throwable throwable)
    {
        this(errorCode, null, throwable);
    }

    public StatdException(ErrorCodeSupplier errorCodeSupplier, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
