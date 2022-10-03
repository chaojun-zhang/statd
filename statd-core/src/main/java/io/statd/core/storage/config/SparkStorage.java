package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.jdbc.SparkDialect;
import io.statd.core.storage.jdbc.SqlDialect;
import io.statd.core.storage.spark.SparkHttpReader;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Objects;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SparkStorage extends JdbcStorage implements MultipleStorage {

    private String baseUrl;

    private List<String> outputs;

    //限制每个结果返回多少条，不设置最多返回5000条
    private int rowLimit;

    @Override
    public Class<? extends StorageReader> getReaderClass() {
        return SparkHttpReader.class;
    }

    @Override
    public JdbcStorageType getStorageType() {
        return JdbcStorageType.Spark;
    }

    public SqlDialect dialect() {
        return new SparkDialect(this);
    }

    @Override
    public void validate() {
        Objects.requireNonNull(sql, "sql not provided");
        Objects.requireNonNull(baseUrl, "baseUrl not provided");
        if (outputs == null || outputs.isEmpty()) {
            throw new IllegalStateException("outputs not provided");
        }

    }

    public boolean disablePreparedStatement() {
        return true;
    }
}

