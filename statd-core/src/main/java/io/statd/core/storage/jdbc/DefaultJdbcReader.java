package io.statd.core.storage.jdbc;

import com.nhl.dflib.DataFrame;
import io.statd.core.dataframe.Table;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import io.statd.core.storage.DataSourceFactory;
import io.statd.core.storage.config.JdbcStorage;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

@Lazy
@Component
@Slf4j
public class DefaultJdbcReader extends JdbcReader {

    @Autowired
    public DefaultJdbcReader(DataSourceFactory jdbcTemplateFactory) {
        super(jdbcTemplateFactory);
    }

    @Override
    public Table read(Query query, JdbcStorage storage) throws StatdException {
        Table table = super.read(query, storage);
        table.setGranularity(query.getGranularity());
        return table;
    }

    @Override
    protected DataFrame getDataFrame(Query query, SqlRender sqlStatement, JdbcStorage jdbcStorage) {
        DataSource dataSource = dataSourceFactory.getDataSource(jdbcStorage.getDatasource());
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(sqlStatement.sql(), query.getQueryParams());

        Calendar calendar;
        if (jdbcStorage.getTimezone() != null) {
            calendar = Calendar.getInstance(TimeZone.getTimeZone(jdbcStorage.getTimezone()));
        } else {
            calendar = Calendar.getInstance();
        }
        return JdbcUtil.toDataFrame(sqlRowSet, (column, value) -> {
            if (StringUtils.isNotEmpty(jdbcStorage.getEventTimeField()) &&
                    column.equals(jdbcStorage.getEventTimeField())) {
                if (jdbcStorage.getTimezone() != null) {
                    Timestamp timestamp = sqlRowSet.getTimestamp(column, calendar);
                    //BUGFIX: gettimestamp后会有纳秒设置，需要清除掉
                    timestamp.setNanos(0);
                    return timestamp;
                } else {
                    return sqlRowSet.getTimestamp(column);
                }
            } else {
                return value;
            }
        });
    }

}
