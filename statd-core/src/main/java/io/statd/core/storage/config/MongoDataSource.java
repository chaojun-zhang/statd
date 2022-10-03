package io.statd.core.storage.config;

import io.statd.core.exception.StatdException;
import lombok.Data;

import java.util.Objects;

@Data
public class MongoDataSource implements Datasource {
    private String url;

    @Override
    public void validate() throws StatdException {
        Objects.requireNonNull(url, "mongo url not provided");
    }
}