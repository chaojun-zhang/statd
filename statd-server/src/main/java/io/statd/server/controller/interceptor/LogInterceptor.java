package io.statd.server.controller.interceptor;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@Slf4j
@Component
public class LogInterceptor implements HandlerInterceptor
{
    private static final String START_TIME = "startTime";
    private static final String THREAD_ID = "threadId";


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object obj) {

        MDC.put(THREAD_ID, String.valueOf(Thread.currentThread().getId()));
        String xReqid = request.getHeader(ReqID.X_LOG_REQ_ID);
        if(StringUtils.isEmpty(xReqid)) {
            xReqid = ReqID.genReqId();
        }
        MDC.put(ReqID.X_LOG_REQ_ID, xReqid);

        request.setAttribute(START_TIME, System.currentTimeMillis());
        log.info("Request client={}, path={}, query={}", request.getRemoteHost(), request.getServletPath(), request.getQueryString());

        return true;
    }


    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        log.info("Processing time={}", System.currentTimeMillis() - (Long)request.getAttribute(START_TIME));
    }

}
