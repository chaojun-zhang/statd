package io.statd.server.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.statd.core.storage.config.Datasource;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class DatasourceConfig {
    private Map<String, Datasource> dataSources = new HashMap<>();

    public void validate() {
        dataSources.values().forEach(Datasource::validate);
    }

    @JsonIgnore
    public Datasource getDatasource(String key) {
        return dataSources.get(key);
    }


}
