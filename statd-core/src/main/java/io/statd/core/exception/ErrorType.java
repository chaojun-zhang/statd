package io.statd.core.exception;

public enum ErrorType
{
    USER_ERROR(0),
    INTERNAL_ERROR(1),
    INSUFFICIENT_RESOURCES(2),
    EXTERNAL(3);

    private final int code;

    ErrorType(int code)
    {
        this.code = code;
    }


    public int getCode()
    {
        return code;
    }
}