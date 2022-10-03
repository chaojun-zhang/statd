package io.statd.core.storage;

import io.statd.core.dataframe.Table;
import io.statd.core.exception.StatdException;
import io.statd.core.storage.config.Storage;
import io.statd.core.query.Query;

import java.util.Map;

public interface StorageReader<T extends Storage> {

    Table read(Query query, T storage) throws StatdException;

    default Map<String, Table> readMore(Query query, T storage) throws StatdException {
        throw new UnsupportedOperationException();
    }
}
