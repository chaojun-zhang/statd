package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.statd.core.storage.StorageReader;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "mode")
@JsonSubTypes({
        @JsonSubTypes.Type(value = GranuleStorage.class, name = "granule"),
        @JsonSubTypes.Type(value = JdbcStorage.class, name = "jdbc"),
        @JsonSubTypes.Type(value = MongoStorage.class, name = "mongo"),
        @JsonSubTypes.Type(value = TimelineStorage.class, name = "timeline"),
        @JsonSubTypes.Type(value = PipelineStorage.class, name = "pipeline"),
        @JsonSubTypes.Type(value = SparkStorage.class, name = "spark")
})
public interface Storage {

    Class<? extends StorageReader> getReaderClass();

    void validate();


}
