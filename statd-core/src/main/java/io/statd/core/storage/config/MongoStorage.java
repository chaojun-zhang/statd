package io.statd.core.storage.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.statd.core.dataframe.Field;
import io.statd.core.dataframe.Schema;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Granularity;
import io.statd.core.storage.mongo.DefaultMongoReader;
import io.statd.core.storage.mongo.MongoPipelineReader;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MongoStorage implements SourceStorage {
    private String datasource;
    //mongo collection
    private String collection;
    //5分钟的时间戳
    private String eventTimeField;
    //合并的时间字段
    private String compactTimeField;
    //"fieldName1 fieldType; fieldName2 fieldType..."
    private String dimensions;
    //"fieldName1 fieldType; fieldName2 fieldType..."
    private String metrics;
    //指定查询,可以覆盖从controller传递的query
    private boolean useMongoAggregate;
    private int maxQueryDays = 7;
    private String timeZone;

    //mongo过滤，采用sql 的过滤表达式
    private String filter;

    @Override
    @JsonIgnore
    public Class getReaderClass() {
        return useMongoAggregate ? MongoPipelineReader.class : DefaultMongoReader.class;
    }

    @JsonIgnore
    public boolean isCompact() {
        return StringUtils.isNotBlank(compactTimeField);
    }

    @Override
    public void validate() throws StatdException {
        Objects.requireNonNull(datasource, "datasource not provided");
        Objects.requireNonNull(collection, "collection not provided");
        Objects.requireNonNull(eventTimeField, "eventTimeField not provided");
        Objects.requireNonNull(metrics, "metrics field not provided");
    }


    public Schema schema() {
        List<Field> fields = new ArrayList<>();
        fields.addAll(dimensionFields());
        fields.addAll(metricFields());
        return new Schema(fields);
    }

    public List<Field> dimensionFields() {
        List<Field> fields = new ArrayList<>();
        if (dimensions != null) {
            String[] dimensionFields = dimensions.split("\\s*,\\s*");
            for (String dimension : dimensionFields) {
                fields.add(Field.createFromString(dimension));
            }
        }
        return fields;
    }

    public List<Field> metricFields() {
        List<Field> fields = new ArrayList<>();
        if (metrics != null) {
            String[] metricFields = metrics.split("\\s*,\\s*");
            for (String metric : metricFields) {
                fields.add(Field.$long(metric));
            }
        }
        return fields;

    }

    @Getter
    @Setter
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class GroupBy {
        private String[] dimensions;
        private String[] metrics;
    }

    public Granularity getGranularity() {
        return Granularity.G5min;
    }
}
