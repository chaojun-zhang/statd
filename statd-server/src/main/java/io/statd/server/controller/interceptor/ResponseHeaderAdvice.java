package io.statd.server.controller.interceptor;


import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;


@ControllerAdvice
@Slf4j
public class ResponseHeaderAdvice implements ResponseBodyAdvice {

    @Override
    public boolean supports(MethodParameter returnType, Class converterType) {
        return true;
    }

    @Override
    public Object beforeBodyWrite(Object body, MethodParameter returnType, MediaType selectedContentType,
                                  Class selectedConverterType, ServerHttpRequest request, ServerHttpResponse response) {
        response.getHeaders().add(ReqID.X_LOG_REQ_ID, MDC.get(ReqID.X_LOG_REQ_ID));
        return body;

    }
}
