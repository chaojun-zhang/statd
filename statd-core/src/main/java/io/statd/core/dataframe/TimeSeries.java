package io.statd.core.dataframe;


import com.nhl.dflib.DataFrame;
import com.nhl.dflib.DataFrameBuilder;
import com.nhl.dflib.DataFrameByRowBuilder;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.row.RowProxy;
import io.statd.core.query.Granularity;
import io.statd.core.query.Interval;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class TimeSeries extends DefaultTable {
    private static final Row EMPTY_ROW = Row.create();
    private final Table input;
    private final LocalDateTime from;
    private final LocalDateTime to;

    private final List<Field> dimensionFields = new ArrayList<>();
    private final List<Field> metricFields = new ArrayList<>();
    private final String fillZeroByField;

    public TimeSeries(Table input, Interval interval,String fillZeroByField, Granularity granularity) {
        super(input.getDataFrame());
        this.input = Objects.requireNonNull(input);
        this.from = interval.getFrom();
        this.to = interval.getTo();

        if (Granularity.isAllGranularity(granularity)) {
            throw new IllegalArgumentException("granularity can not be null");
        }
        this.setGranularity(granularity);


        for (Field field : input.getSchema().getFields()) {
            if (FieldType.STRING == field.getType()) {
                this.dimensionFields.add(field);
            }
        }


        for (Field field : input.getSchema().getFields()) {
            if (dimensionFields.contains(field.getName())) {
                continue;
            }
            if (FieldType.INT == field.getType()
                    || FieldType.FLOAT == field.getType()
                    || FieldType.DOUBLE == field.getType()
                    || FieldType.LONG == field.getType()) {
                this.metricFields.add(field);
            }
        }

        if (StringUtils.isNotEmpty(fillZeroByField)) {
            this.fillZeroByField = fillZeroByField;
        } else {
            if (!input.getSchema().getEventTimeField().isPresent()) {
                throw new IllegalStateException("eventTime field not found in input table");
            }
            this.fillZeroByField = input.getSchema().getEventTimeField().get().getName();
        }

        if (this.metricFields.isEmpty()) {
            throw new IllegalStateException("metric field not found in input table");
        }
    }

    /**
     * 构建时序数据，先找出所有缺失维度和缺失的时间进行填充
     *
     * @return timestamp->dimension->metric
     */
    private Map<LocalDateTime, Map<Row, Row>> fillZero() {
        String eventTimeField = this.fillZeroByField;
        final Map<LocalDateTime, Map<Row, Row>> eventToDimensionMetric = new HashMap<>();
        Set<Row> dimensionRows = new HashSet<>();
        input.getDataFrame().forEach(row -> {
            Timestamp eventTimeValue = (Timestamp) row.get(eventTimeField);
            LocalDateTime timestamp = eventTimeValue.toLocalDateTime()
                    .truncatedTo(ChronoUnit.MINUTES);
            Row dimension = createDimensionRow(row);
            Row metric = createMetricRow(row);
            eventToDimensionMetric.compute(timestamp, (k, v) -> {
                if (v == null) {
                    v = new HashMap<>();
                }
                v.put(dimension, metric);
                return v;
            });
            dimensionRows.add(dimension);
        });

        //修复没有数据补0问题,只针对用时间维度的补0
        if (dimensionRows.isEmpty()) {
            if (dimensionFields.isEmpty()) {
                dimensionRows.add(EMPTY_ROW);
            } else {
                return Collections.emptyMap();
            }
        }

        LocalDateTime start = this.getGranularity().getDateTime(from);
        while (start.isBefore(to)) {
            //fill missing time
            Map<Row, Row> dimensionMetric = eventToDimensionMetric.computeIfAbsent(start, k -> createEmptyDimensionMetrics(dimensionRows));
            for (Row dimension : dimensionRows) {
                //fill missing dimension
                dimensionMetric.computeIfAbsent(dimension, k -> createEmptyMetric());
            }
            eventToDimensionMetric.put(start, dimensionMetric);
            start = this.getGranularity().nextPeriod(start);
        }


        return eventToDimensionMetric;
    }

    public DataFrame getDataFrame() {
        String[] columnLabels = input.getSchema().allColumnLabels();
        Accumulator[] accumulators = input.getSchema().accumulators();

        DataFrameByRowBuilder dataFrameByRowBuilder = DataFrameBuilder.builder(columnLabels).byRow(accumulators);
        Map<LocalDateTime, Map<Row, Row>> rows = fillZero();
        for (Map.Entry<LocalDateTime, Map<Row, Row>> entry : rows.entrySet()) {
            for (Map.Entry<Row, Row> r : entry.getValue().entrySet()) {
                Object[] value = new Object[]{Timestamp.valueOf(entry.getKey())};
                value = ArrayUtils.addAll(value, r.getKey().getData());
                value = ArrayUtils.addAll(value, r.getValue().getData());
                dataFrameByRowBuilder.addRow(value);
            }
        }
        DataFrame dataFrame = dataFrameByRowBuilder.create();
        return dataFrame.sort(this.fillZeroByField, true);
    }

    @Override
    public Schema getSchema() {
        return input.getSchema();
    }


    private Row createMetricRow(RowProxy row) {
        Object[] rowObject = metricFields.stream().map(it -> row.get(it.getName())).toArray();
        return Row.create(rowObject);
    }

    private Row createDimensionRow(RowProxy row) {
        if (CollectionUtils.isEmpty(dimensionFields)) {
            return EMPTY_ROW;
        }
        Object[] rowObject = dimensionFields.stream().map(it -> row.get(it.getName())).toArray();
        return Row.create(rowObject);
    }

    private Map<Row, Row> createEmptyDimensionMetrics(Set<Row> dimensions) {
        Map<Row, Row> dimensionMetrics = new HashMap<>();
        dimensions.forEach(dimension -> {
            Row metric = createEmptyMetric();
            dimensionMetrics.put(dimension, metric);
        });
        return dimensionMetrics;
    }

    private Row createEmptyMetric() {
        Object[] objects = metricFields.stream().map(it -> {
            if (FieldType.INT == it.getType()) {
                return (Object) 0;
            } else if (FieldType.LONG == it.getType()) {
                return 0L;
            } else if (FieldType.DOUBLE == it.getType()) {
                return 0.;
            } else if (FieldType.FLOAT == it.getType()) {
                return 0.f;
            } else {
                throw new IllegalStateException("Unexpected value: " + input.getDataFrame().getColumn(it.getName()).getNominalType());
            }
        }).toArray();
        return Row.create(objects);
    }

}
