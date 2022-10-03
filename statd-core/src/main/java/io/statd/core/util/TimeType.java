package io.statd.core.util;

import lombok.Getter;

import java.util.stream.Stream;

public enum TimeType {
    TS_SECOND("SECOND"), TS_MILLI("MILLI"), FORMAT("FORMAT"), DATETIME("DATETIME");

    @Getter
    private String type;

    TimeType(String type) {
        this.type = type;
    }

    public static TimeType fromString(String value) {
        return Stream.of(values())
                .filter(v -> v.type.equalsIgnoreCase(value))
                .findFirst()
                .orElse(FORMAT);
    }
}