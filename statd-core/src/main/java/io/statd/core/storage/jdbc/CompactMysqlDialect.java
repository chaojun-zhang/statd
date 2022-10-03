package io.statd.core.storage.jdbc;

import io.statd.core.query.Granularity;
import io.statd.core.storage.config.JdbcStorage;
import io.statd.core.storage.config.compact.CompactConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

public class CompactMysqlDialect extends MysqlDialect{

    private CompactConfig compactConfig;

    public CompactMysqlDialect(JdbcStorage storage) {
        super(storage);
        this.compactConfig = storage.getCompact();
    }

    @Override
    public Optional<String> timePeriod(Granularity granularity) {
        return Optional.of(storage.getEventTimeField());
    }

    @Override
    public String timeRange(LocalDateTime start, LocalDateTime end) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(compactConfig.timePattern);
        int s = Integer.parseInt(dateTimeFormatter.format(start));
        int e = Integer.parseInt(dateTimeFormatter.format(end));
        return String.format("%s >= %s and %s < %s", storage.getEventTimeField(), s, storage.getEventTimeField(), e);
    }
}
