package io.statd.server.controller.interceptor;

import io.statd.core.exception.StatdException;
import io.statd.server.controller.ResultBody;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import javax.servlet.http.HttpServletRequest;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(value = StatdException.class)
    @ResponseBody
    @ResponseStatus(code = HttpStatus.BAD_REQUEST)
    public ResultBody exceptionHandler(HttpServletRequest req, StatdException e) {
        log.error("statd error:", e);
        return ResultBody.error(String.format("%s(%s)", e.getErrorCode().getName(), e.getErrorCode().getCode()), e.getMessage());
    }

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    @ResponseStatus(code = HttpStatus.INTERNAL_SERVER_ERROR)
    public ResultBody exceptionHandler(HttpServletRequest req, Exception e) {
        log.error("Unknown error:", e);
        return ResultBody.error("500", "Unknown server error");
    }

}
