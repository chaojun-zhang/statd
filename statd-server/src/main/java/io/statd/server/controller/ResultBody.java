package io.statd.server.controller;

import lombok.Data;

@Data
public class ResultBody {

    private Object data;

    private String code;

    private String message;

    public static ResultBody error(String code, String message) {
        ResultBody result = new ResultBody();
        result.setMessage(message);
        result.setCode(code);
        return result;
    }

    public static ResultBody ok(Object body) {
        ResultBody result = new ResultBody();
        result.setData(body);
        result.setCode("200");
        result.setMessage("OK");
        return result;
    }
}
