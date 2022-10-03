package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.pipeline.PipelineReader;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PipelineStorage implements MultipleStorage {

    private Map<String, String> params = new HashMap<>();
    private Map<String, String> functions = new HashMap<>();
    private Map<String, SingleStorage> input = new HashMap<>();
    private String transform;
    private List<Output> output = new ArrayList<>();

    @JsonIgnore
    @Override
    public Class<? extends StorageReader> getReaderClass() {
        return PipelineReader.class;
    }


    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Output {
        private String dataFrameName;
        private String name;
        private boolean fillZero;
        private String fillZeroByField;
        private Map<String, String> props;

        public void validate() {
            Objects.requireNonNull(dataFrameName);
        }

        public String getName() {
            if (name == null) {
                return dataFrameName;
            } else {
                return name;
            }
        }

        public List<String> getList(String key) {
            if (props == null) {
                return new ArrayList<>();
            } else {
                String value = props.get(key);
                if (value != null) {
                    String[] split = value.split("\\s*,\\s*");
                    return Arrays.asList(split);
                }
                return new ArrayList<>();
            }
        }

    }

    @Override
    public void validate() {
        if (input == null || input.isEmpty()) {
            throw new IllegalStateException("pipeline input required");
        } else {
            input.forEach((name, storage) -> storage.validate());
        }


        if (output == null || output.isEmpty()) {
            throw new IllegalStateException("at least one pipeline output required");
        } else {
            output.forEach(it -> {
                it.validate();
                if (transform == null) {
                    if (!input.containsKey(it.getDataFrameName())) {
                        throw new IllegalStateException("dataFrameName '" + it.getDataFrameName() + "' not found in input");
                    }
                }
            });

        }

    }

}
