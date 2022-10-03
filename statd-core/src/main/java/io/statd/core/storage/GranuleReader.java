package io.statd.core.storage;

import io.statd.core.dataframe.Table;
import io.statd.core.exception.StatdException;
import io.statd.core.exception.StorageConfigException;
import io.statd.core.query.Query;
import io.statd.core.storage.config.GranuleStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Lazy
@Component
public class GranuleReader implements StorageReader<GranuleStorage> {

    private final DataFrameLoader datasetReader;

    @Autowired
    public GranuleReader(DataFrameLoader datasetReader) {
        this.datasetReader = datasetReader;
    }

    @Override
    public Table read(Query query, GranuleStorage storage) throws StatdException {
        switch (query.getGranularity()) {
            case GMin:
                if (storage.getOneMinute() == null) {
                    throw new StorageConfigException("1min not provided in config");
                }
                return datasetReader.load(query, storage.getOneMinute());
            case G5min:
                if (storage.getFiveMinute() == null) {
                    throw new StorageConfigException("5min not provided in config");
                }
                return datasetReader.load(query, storage.getFiveMinute());
            case GHour:
                if (storage.getHour() == null) {
                    throw new StorageConfigException("hour not provided in config");
                }
                return datasetReader.load(query, storage.getHour());
            case GDay:
                if (storage.getDay() == null) {
                    throw new StorageConfigException("day not provided in config");
                }
                return datasetReader.load(query, storage.getDay());
            case GWeek:
                if (storage.getWeek() == null) {
                    throw new StorageConfigException("week not provided in config");
                }
                return datasetReader.load(query, storage.getWeek());
            case GMonth:
                if (storage.getMonth() == null) {
                    throw new StorageConfigException("month not provided in config");
                }
                return datasetReader.load(query, storage.getMonth());
            case GQuarter:
                if (storage.getQuarter() == null) {
                    throw new StorageConfigException("quarter not provided in config");
                }
                return datasetReader.load(query, storage.getQuarter());
            case GYear:
                if (storage.getYear() == null) {
                    throw new StorageConfigException("year not provided in config");
                }
                return datasetReader.load(query, storage.getYear());
            default:
                throw new IllegalArgumentException(String.format("granule '%s' is not supported", query.getGranularity()));
        }
    }


}
