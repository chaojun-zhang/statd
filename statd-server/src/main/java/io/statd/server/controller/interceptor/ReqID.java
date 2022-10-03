package io.statd.server.controller.interceptor;


import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Base64;

public class ReqID {
    public static final String X_LOG_REQ_ID = "X-Reqid";
    private static long pid;
    {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        String jvmName = bean.getName();
        pid = Long.parseLong(jvmName.split("@")[0]);
    }

    public static String genReqId() {
        byte[] b = new byte[12];
        b[0] = (byte) pid;
        b[1] = (byte) (pid >> 8);
        b[2] = (byte) (pid >> 16);
        b[3] = (byte) (pid >> 24);

        long nano = System.nanoTime();
        b[4] = (byte) nano;
        b[5] = (byte) (nano >> 8);
        b[6] = (byte) (nano >> 16);
        b[7] = (byte) (nano >> 24);
        b[8] = (byte) (nano >> 32);
        b[9] = (byte) (nano >> 40);
        b[10] = (byte) (nano >> 48);
        b[11] = (byte) (nano >> 56);

        return Base64.getUrlEncoder().encodeToString(b);
    }
}
