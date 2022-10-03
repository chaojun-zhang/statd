package io.statd.core.storage.jdbc;

import io.statd.core.query.Granularity;
import io.statd.core.storage.config.JdbcStorage;
import io.statd.core.util.TimeType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

public final class TrinoDialect extends SqlDialect {
    private final JdbcStorage storage;

    public TrinoDialect(JdbcStorage storage) {
        this.storage = storage;
    }

    @Override
    public Optional<String> timePeriod(Granularity granularity) {
        String timeFieldType = Optional.ofNullable(storage.getTimeType()).orElse("SECOND");
        TimeType timeType = TimeType.fromString(timeFieldType);
        String datetimeExpr;
        //https://trino.io/docs/current/functions/datetime.html#
        if (timeType == TimeType.TS_SECOND) {
            datetimeExpr = String.format("from_unixtime(%s)", storage.getEventTimeField());
        } else if (timeType == TimeType.TS_MILLI) {
            datetimeExpr = String.format("from_unixtime(%s /1000)", storage.getEventTimeField());
        } else if (timeType == TimeType.FORMAT) {
            datetimeExpr = String.format("parse_datetime(cast(%s as string),'%s')", storage.getEventTimeField(), timeFieldType);
        } else {
            throw new UnsupportedOperationException("unsupported time type: " + timeType);
        }

        if (Granularity.isAllGranularity(granularity)) {
            return Optional.empty();
        }
        switch (granularity) {
            case GMin:
            case G5min:
                return Optional.of(String.format("DATE_TRUNC('minute', %s)", datetimeExpr));
            case GHour:
                return Optional.of(String.format("DATE_TRUNC('hour', %s)", datetimeExpr));
            case GDay:
                return Optional.of(String.format("DATE_TRUNC('day', %s)", datetimeExpr));
            case GWeek:
                return Optional.of(String.format("DATE_TRUNC('week', %s)", datetimeExpr));
            case GMonth:
                return Optional.of(String.format("DATE_TRUNC('month', %s)", datetimeExpr));
            case GQuarter:
                return Optional.of(String.format("DATE_TRUNC('quarter', %s)", datetimeExpr));
            case GYear:
                return Optional.of(String.format("DATE_TRUNC('year', %s)", datetimeExpr));
            default:
                throw new UnsupportedOperationException(String.format("date truncate function is not supported on granularity %s", granularity));
        }
    }

    @Override
    public String timeRange(LocalDateTime start, LocalDateTime end) {
        String timeFieldType = Optional.ofNullable(storage.getTimeType()).orElse( "SECOND");
        TimeType timeType = TimeType.fromString(timeFieldType);
        if (timeType == TimeType.TS_MILLI) {
            long startTime =  Timestamp.valueOf(start).getTime()/1000;
            long endTime =  Timestamp.valueOf(end).getTime()/1000;
            return String.format("%s >= %s and %s < %s", storage.getEventTimeField(), startTime, storage.getEventTimeField(), endTime);
        } else if (timeType == TimeType.TS_SECOND) {
            long startTime = Timestamp.valueOf(start).getTime();
            long endTime = Timestamp.valueOf(end).getTime();
            return String.format("%s >= %s and %s < %s", storage.getEventTimeField(), startTime, storage.getEventTimeField(), endTime);
        } else  {
            throw new UnsupportedOperationException("timeType:'" + timeType + "' is not supported");
        }
    }


}
