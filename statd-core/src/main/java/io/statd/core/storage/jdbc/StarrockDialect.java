package io.statd.core.storage.jdbc;

import io.statd.core.query.Granularity;
import io.statd.core.storage.config.JdbcStorage;
import io.statd.core.util.TimeType;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class StarrockDialect extends SqlDialect {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
    protected final JdbcStorage storage;

    public StarrockDialect(JdbcStorage storage) {
        this.storage = storage;
    }

    @Override
    public Optional<String> timePeriod(Granularity granularity) {
        if (Granularity.isAllGranularity(granularity)) {
            return Optional.empty();
        }
        String timestampField = storage.getEventTimeField();
        String timeFieldType = Optional.ofNullable(storage.getTimeType()).orElse(TimeType.DATETIME.getType());
        TimeType timeType = TimeType.fromString(timeFieldType);
        if (timeType == TimeType.TS_SECOND) {
            timestampField = String.format("from_unixtime(%s)", storage.getEventTimeField());
        } else if (timeType == TimeType.TS_MILLI) {
            timestampField = String.format("from_unixtime(%s /1000)", storage.getEventTimeField());
        } else if (timeType == TimeType.FORMAT) {
            //如果是数字，先转成字符串,然后在转回日期
            timestampField = String.format("str_to_date(cast(%s as string),'%s')", storage.getEventTimeField(), timeFieldType);
        }

        switch (granularity) {
            case GMin:
                return Optional.of(String.format("timestamp(from_unixtime(unix_timestamp(%s) - unix_timestamp(%s) %s))", timestampField, timestampField, "% 60"));
            case G5min:
                return Optional.of(String.format("timestamp(from_unixtime(unix_timestamp(%s) - unix_timestamp(%s) %s))", timestampField, timestampField, "% 300"));
            case GHour:
                return Optional.of(String.format("timestamp(from_unixtime(unix_timestamp(%s) - unix_timestamp(%s) %s))", timestampField, timestampField, "% 3600"));
            case GDay:
                return Optional.of(String.format("cast(cast(%s as date) as datetime)", timestampField));
            case GWeek:
                timestampField = String.format("cast(cast(%s as date) as datetime)", timestampField);
                return Optional.of(String.format("date_sub(%s,INTERVAL dayofweek(%s) DAY)", timestampField, timestampField));
            case GMonth:
                timestampField = String.format("cast(cast(%s as date) as datetime)", timestampField);
                return Optional.of(String.format("date_sub(%s,INTERVAL DAY(%s) - 1 day) ", timestampField, timestampField));
            case GQuarter:
                timestampField = String.format("cast(cast(%s as date) as datetime)", timestampField);
                return Optional.of(String.format("select MAKEDATE(EXTRACT(YEAR FROM %s),1) + interval QUARTER(%s)*3-3 month", timestampField, timestampField));
            case GYear:
                return Optional.of(String.format("MAKEDATE(EXTRACT(YEAR FROM %s),1) ", timestampField));
            default:
                throw new UnsupportedOperationException(String.format("date truncate function is not supported on granularity %s", granularity));
        }

    }

    @Override
    public String timeRange(LocalDateTime start, LocalDateTime end) {
        String startTime = formatter.format(start);
        String endTime = formatter.format(end);
        return String.format("%s >= '%s' and %s < '%s'", storage.getEventTimeField(), startTime, storage.getEventTimeField(), endTime);
    }

}
