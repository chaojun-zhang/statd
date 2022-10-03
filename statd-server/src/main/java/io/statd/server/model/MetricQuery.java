package io.statd.server.model;

import io.statd.core.query.Query;
import lombok.Getter;
import org.apache.commons.lang.builder.HashCodeBuilder;


public class MetricQuery {
    @Getter
    private final Query query;
    @Getter
    private final ModuleMetric moduleMetric;

    public MetricQuery(Query query, ModuleMetric moduleMetric) {
        this.query = query;
        this.moduleMetric = moduleMetric;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricQuery that = (MetricQuery) o;
        return query.equals(that.query) && moduleMetric.equals(that.moduleMetric);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(query)
                .append(moduleMetric)
                .toHashCode();
    }
}
