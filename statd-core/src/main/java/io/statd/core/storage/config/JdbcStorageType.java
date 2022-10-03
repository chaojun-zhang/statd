package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;

public enum JdbcStorageType {
    MySql("mysql"),Starrocks("starrocks"), Druid("druid"), Trino("trino"),Spark("spark");

    @Getter
    private String type;

    JdbcStorageType(String type) {
        this.type = type;
    }

    @JsonValue
    public String getType() {
        return type;
    }

    public static JdbcStorageType of(String value) {
        if (value == null) {
            return Druid;
        }
        switch (value.toLowerCase()) {
            case "mysql":
                return MySql;
            case "druid":
                return Druid;
            case "trino":
                return Trino;
            case "spark":
                return Spark;
            case "starrocks":
                return Starrocks;
            default:
                throw new IllegalStateException("Unexpected value: " + value.toLowerCase());
        }
    }
}
