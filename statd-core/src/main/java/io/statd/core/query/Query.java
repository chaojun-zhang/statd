package io.statd.core.query;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;


@Data
@NoArgsConstructor
public class Query {
    private final static AtomicLong REQ_ID = new AtomicLong();
    private LocalDateTime from;
    private LocalDateTime to;
    private Granularity granularity = Granularity.GAll;
    private List<String> dimensions = new ArrayList<>();
    private List<String> metrics = new ArrayList<>();
    private Map<String, Object> queryParams = new LinkedHashMap<>();

    public Interval getInterval() {
        return new Interval(from, to);
    }

    public long getQueryId() {
        return REQ_ID.incrementAndGet();
    }

    public Query(Query query) {
        this.from = query.from;
        this.to = query.to;
        this.granularity = query.granularity;
        this.dimensions = query.dimensions;
        this.metrics = query.metrics;
    }

    public Map<String, Object> templateParams() {
        Map<String, Object> params = new HashMap<>(queryParams);
        if (!Granularity.isAllGranularity(this.getGranularity())) {
            params.put("g", this.getGranularity().getName());
        }
        params.put("from", Timestamp.valueOf(this.getFrom()).getTime());
        params.put("to", Timestamp.valueOf(this.getTo()).getTime());
        //query里面有维度，构造维度参数，放到freemarker的渲染变量
        if (!this.dimensions.isEmpty()) {
            params.put("dimension", String.join(",", this.getDimensions()));
        }
        //query里面有指标，构造指标参数，放到freemarker的渲染变量
        if (!this.metrics.isEmpty()) {
            params.put("metric", String.join(",", this.getMetrics()));
        }
        params.put("queryId", getQueryId());
        int slots = new Interval(this.getFrom(), this.getTo()).slots(Granularity.G5min);
        params.put("slots", slots);
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Query query = (Query) o;
        return from.equals(query.from)
                && to.equals(query.to)
                && granularity == query.granularity
                && Objects.equals(dimensions, query.dimensions)
                && Objects.equals(metrics, query.metrics)
                && Objects.equals(queryParams, query.queryParams);
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hashCode = new HashCodeBuilder()
                .append(from)
                .append(to);

        if (!Granularity.isAllGranularity(getGranularity())) {
            hashCode = hashCode.append(granularity);
        }
        if (dimensions != null && !dimensions.isEmpty()) {
            hashCode = hashCode.append(dimensions);
        }
        if (metrics != null && !metrics.isEmpty()) {
            hashCode = hashCode.append(metrics);
        }
        if (queryParams != null && !queryParams.isEmpty()) {
            hashCode = hashCode.append(queryParams);
        }
        return hashCode.toHashCode();
    }
}
