package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.statd.core.storage.GranuleReader;
import lombok.Data;

import java.util.Objects;
import java.util.stream.Stream;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GranuleStorage implements SingleStorage {

    @JsonProperty("1min")
    private SingleStorage oneMinute;
    @JsonProperty("5min")
    private SingleStorage fiveMinute;
    @JsonProperty("hour")
    private SingleStorage hour;
    @JsonProperty("day")
    private SingleStorage day;
    @JsonProperty("week")
    private SingleStorage week;
    @JsonProperty("month")
    private SingleStorage month;
    @JsonProperty("quarter")
    private SingleStorage quarter;
    @JsonProperty("year")
    private SingleStorage year;

    @Override
    @JsonIgnore
    public Class<GranuleReader> getReaderClass() {
        return GranuleReader.class;
    }


    @Override
    public void validate() {
        boolean atLeastOneExists = Stream.of(oneMinute, fiveMinute, hour, day, week, month, quarter, year).anyMatch(Objects::nonNull);
        if (!atLeastOneExists) {
            throw new IllegalStateException("at least one of granule storage is required");
        }
        Stream.of(oneMinute, fiveMinute, hour, day, week, month, quarter, year).filter(Objects::nonNull).forEach(Storage::validate);

    }


}
