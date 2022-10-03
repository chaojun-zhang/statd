package io.statd.core.query;

import lombok.Data;

import java.util.HashMap;

@Data
public class Record extends HashMap<String, Object> {
    @Override
    public String toString() {
        return super.toString();
    }
}
