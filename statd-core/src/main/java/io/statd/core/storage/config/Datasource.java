package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = JdbcDataSource.class, name = "jdbc"),
        @JsonSubTypes.Type(value = MongoDataSource.class, name = "mongo")
})
public interface Datasource {
    void validate();
}