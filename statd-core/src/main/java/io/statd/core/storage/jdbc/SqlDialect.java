package io.statd.core.storage.jdbc;

import io.statd.core.query.Granularity;

import java.time.LocalDateTime;
import java.util.Optional;

public abstract class SqlDialect {

    abstract public Optional<String> timePeriod(Granularity granularity);

    abstract public String timeRange(LocalDateTime start, LocalDateTime end);

}

