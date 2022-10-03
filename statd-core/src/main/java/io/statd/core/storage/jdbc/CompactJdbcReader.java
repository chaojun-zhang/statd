package io.statd.core.storage.jdbc;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.accumulator.LongAccumulator;
import com.nhl.dflib.accumulator.TimestampAccumulator;
import io.statd.core.dataframe.Table;
import io.statd.core.dflib.AccumulatorFactory;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Granularity;
import io.statd.core.query.Query;
import io.statd.core.storage.DataSourceFactory;
import io.statd.core.storage.config.JdbcStorage;
import io.statd.core.storage.config.compact.CompactConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Lazy
@Component
@Slf4j
public class CompactJdbcReader extends JdbcReader {

    @Autowired
    public CompactJdbcReader(DataSourceFactory jdbcTemplateFactory) {
        super(jdbcTemplateFactory);
    }

    @Override
    public Table read(Query query, JdbcStorage storage) throws StatdException {
        Table table =  super.read(query, storage);
        table.setGranularity(Granularity.G5min);
        return table;
    }

    @Override
    protected DataFrame getDataFrame(Query query, SqlRender sqlStatement, JdbcStorage jdbcStorage) {
        NamedParameterJdbcTemplate jdbcTemplate = new NamedParameterJdbcTemplate(dataSourceFactory.getDataSource(jdbcStorage.getDatasource()));
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(sqlStatement.sql(), query.getQueryParams());
        CompactConfig compact = jdbcStorage.getCompact();
        return buildDataFrame(sqlRowSet, compact, jdbcStorage);
    }

    public DataFrame buildDataFrame(SqlRowSet rs, CompactConfig compactConfig, JdbcStorage jdbcStorage) {
        SqlRowSetMetaData rsmd = rs.getMetaData();
        List<String> columnLabels = new ArrayList<>();
        List<String> slotColumns = compactConfig.slotColumns();
        Map<String, Accumulator> accums = new HashMap<>();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            String cl = rsmd.getColumnLabel(i);
            //初始的时间字段和slot列不需要
            if (jdbcStorage.getEventTimeField().equalsIgnoreCase(cl) || slotColumns.contains(cl)) {
                continue;
            } else {
                columnLabels.add(cl);
                accums.put(cl, AccumulatorFactory.get(rsmd.getColumnType(i)));
            }
        }
        accums.put(compactConfig.targetFieldName, new TimestampAccumulator());
        accums.put(compactConfig.metricName, new LongAccumulator());
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(compactConfig.getTimePattern());
        while (rs.next()) {
            for (int i = 0; i < slotColumns.size(); i++) {
                for (String col : columnLabels) {
                    Object o = rs.getObject(col);
                    accums.get(col).add(o);
                }
                String slotColumn = slotColumns.get(i);
                Timestamp timeStamp = getTimeStamp(rs, i, dateTimeFormatter, jdbcStorage, compactConfig.timeType());
                accums.get(compactConfig.targetFieldName).add(timeStamp);
                accums.get(compactConfig.metricName).add(((Number) rs.getObject(slotColumn)).longValue());
            }
        }

        columnLabels.add(compactConfig.targetFieldName);
        columnLabels.add(compactConfig.metricName);
        List<Series> series = new ArrayList<>();
        for (String s : columnLabels) {
            series.add(accums.get(s).toSeries());
        }
        return DataFrame.newFrame(columnLabels.toArray(new String[0])).columns(series.toArray(new Series[0]));

    }

    public Timestamp getTimeStamp(SqlRowSet rs, int slot, DateTimeFormatter formater, JdbcStorage jdbcStorage, CompactConfig.TimeType timeType) {
        if (CompactConfig.TimeType.DAY_INT == timeType) {
            String timeField = Objects.requireNonNull(rs.getString(jdbcStorage.getEventTimeField()));
            LocalDate day = LocalDate.parse(timeField, formater);
            LocalDateTime minute = LocalDateTime.of(day, LocalTime.MIN).plusMinutes(slot * 5L);
            return Timestamp.valueOf(minute);
        } else {
            String timeField = Objects.requireNonNull(rs.getString(jdbcStorage.getEventTimeField()));
            LocalDateTime hour = LocalDateTime.parse(timeField, formater);
            LocalDateTime minute = hour.plusMinutes(slot * 5L);
            return Timestamp.valueOf(minute);
        }
    }

}
