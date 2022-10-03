package io.statd.core.exception;


public enum QueryRequestError implements ErrorCodeSupplier {

    InvalidTimeFormat(5000, "Invalid_Time_Format"),
    InvalidGranule(5001, "Invalid_Granularity"),
    InvalidInterval(5002, "Invalid_Time_Range"),
    InvalidQueryParams(5003, "Invalid_Query_Params"),
    InvalidFilter(5004, "InvalidFilter"),
    InvalidQueryModule(5005, "Invalid_Query_Module"),
    InvalidQueryMetric(5006, "Invalid_Query_Metric"),
    QueryMetricNotFound(5007, "Metric_Not_Found");

    private final ErrorCode errorCode;

    QueryRequestError(int code, String name) {
        this.errorCode = new ErrorCode(code, name, ErrorType.USER_ERROR);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
