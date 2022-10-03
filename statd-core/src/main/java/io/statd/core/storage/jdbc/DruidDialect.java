package io.statd.core.storage.jdbc;

import io.statd.core.query.Granularity;
import io.statd.core.storage.config.JdbcStorage;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Optional;

public final class DruidDialect extends SqlDialect {
    private final JdbcStorage storage;

    public DruidDialect(JdbcStorage storage) {
        this.storage = storage;
    }

    @Override
    public Optional<String> timePeriod(Granularity granularity) {
        if (Granularity.isAllGranularity(granularity)) {
            return Optional.empty();
        }
        String originalTime = storage.getEventTimeField();
        if (storage.getZoneOffsetHour() != 0) {
            originalTime = String.format("TIMESTAMPADD(hour,-%s,%s)", storage.getZoneOffsetHour(), originalTime);
        }
        switch (granularity) {
            case GMin:
            case G5min:
                return Optional.of(String.format("DATE_TRUNC('minute', %s)",originalTime));
            case GHour:
                return Optional.of(String.format("DATE_TRUNC('hour', %s)", originalTime));
            case GDay:
                return Optional.of(String.format("TIMESTAMPADD(hour,-8,DATE_TRUNC('day', TIMESTAMPADD(hour,8,%s)))", originalTime));
            case GWeek:
                return Optional.of(String.format("TIMESTAMPADD(hour,-8,DATE_TRUNC('week',TIMESTAMPADD(hour,8, %s)))", originalTime));
            case GMonth:
                return Optional.of(String.format("TIMESTAMPADD(hour,-8,DATE_TRUNC('month', TIMESTAMPADD(hour,8,%s)))", originalTime));
            case GQuarter:
                return Optional.of(String.format("TIMESTAMPADD(hour,-8,DATE_TRUNC('quarter', TIMESTAMPADD(hour,8,%s)))", originalTime));
            case GYear:
                return Optional.of(String.format("TIMESTAMPADD(hour,-8,DATE_TRUNC('year', TIMESTAMPADD(hour,8,%s)))",originalTime));
            default:
                throw new UnsupportedOperationException(String.format("date truncate function is not supported on granularity %s", granularity));
        }

    }

    @Override
    public String timeRange(LocalDateTime start, LocalDateTime end) {
        String startTime = String.format("MILLIS_TO_TIMESTAMP(%s)", Timestamp.valueOf(start).getTime());
        String endTime = String.format("MILLIS_TO_TIMESTAMP(%s)", Timestamp.valueOf(end).getTime());
        return String.format("%s >= %s and %s < %s", storage.getEventTimeField(), startTime, storage.getEventTimeField(), endTime);

    }
}
