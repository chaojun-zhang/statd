package io.statd.core.storage;

import io.statd.core.dataframe.Table;
import io.statd.core.query.Query;
import io.statd.core.storage.config.MultipleStorage;
import io.statd.core.storage.config.SingleStorage;

import java.util.Map;


public interface DataFrameLoader {

    Table load(Query search, SingleStorage storage);

    Map<String, Table> load(Query search, MultipleStorage storage);

}
