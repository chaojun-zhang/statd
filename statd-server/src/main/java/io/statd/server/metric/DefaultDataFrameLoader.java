package io.statd.server.metric;

import io.statd.core.dataframe.Table;
import io.statd.core.query.Query;
import io.statd.core.storage.DataFrameLoader;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.config.MultipleStorage;
import io.statd.core.storage.config.SingleStorage;
import io.statd.core.storage.config.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;


@Slf4j
@Component
public class DefaultDataFrameLoader implements ApplicationContextAware, DataFrameLoader {

    private ApplicationContext applicationContext;

    private StorageReader getReader(Storage storage) {
        return applicationContext.getBean(storage.getReaderClass());
    }

    public Table load(Query search, SingleStorage storage) {
        return getReader(storage).read(search, storage);
    }

    public Map<String, Table> load(Query search, MultipleStorage storage) {
        return getReader(storage).readMore(search, storage);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
