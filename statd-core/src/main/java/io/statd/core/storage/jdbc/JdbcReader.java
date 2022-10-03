package io.statd.core.storage.jdbc;

import com.nhl.dflib.DataFrame;
import io.statd.core.dataframe.Table;
import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import io.statd.core.storage.DataSourceFactory;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.config.JdbcStorage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class JdbcReader implements StorageReader<JdbcStorage> {

    protected final DataSourceFactory dataSourceFactory;

    public JdbcReader(DataSourceFactory dataSourceFactory) {
        this.dataSourceFactory = dataSourceFactory;
    }

    @Override
    public Table read(Query query, JdbcStorage storage) throws StatdException {
        if (query.getInterval().days().size() > storage.getMaxQueryDays()) {
            throw new StatdException(QueryRequestError.InvalidInterval, "Query time range cannot exceed " + storage.getMaxQueryDays() + " days");
        }

        SqlRender sqlRender = SqlRender.create(query, storage);
        log.info("sql:'{}', args: {}", sqlRender.sql(), query.getQueryParams());
        DataFrame dataFrame = getDataFrame(query, sqlRender, storage);
        return Table.create(dataFrame);
    }


    protected abstract DataFrame getDataFrame(Query query, SqlRender sqlStatement, JdbcStorage jdbcStorage);

}
