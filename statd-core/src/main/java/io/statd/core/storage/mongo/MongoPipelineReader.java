package io.statd.core.storage.mongo;

import com.nhl.dflib.accumulator.Accumulator;
import io.statd.core.dataframe.Field;
import io.statd.core.dataframe.Schema;
import io.statd.core.dataframe.Table;
import io.statd.core.exception.StatdException;
import io.statd.core.parser.filter.FilterCompiler;
import io.statd.core.query.Granularity;
import io.statd.core.query.Query;
import io.statd.core.storage.DataSourceFactory;
import io.statd.core.storage.config.MongoStorage;
import io.statd.core.storage.template.TemplateUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators;
import org.springframework.data.mongodb.core.aggregation.DateOperators;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * 使用pipeline方式读取mongo
 */
@Slf4j
@Component
@Lazy
public class MongoPipelineReader extends MongoReader {

    private static final String YEAR = "_year_";
    private static final String MONTH = "_month_";
    private static final String DAY = "_day_";
    private static final String HOUR = "_hour_";
    private static final String WEEK = "_week_";
    private static final String MINUTE = "_minute_";

    @Autowired
    public MongoPipelineReader(DataSourceFactory dataSourceFactory) {
        super(dataSourceFactory);
    }

    @Override
    public Table read(Query search, MongoStorage storage) throws StatdException {
        Table table = super.read(search, storage);
        table.setGranularity(search.getGranularity());
        return table;
    }

    protected Iterator<Document> fetchDocuments(Query search, MongoStorage storage, MongoTemplate mongoTemplate) {

        Aggregation aggregation = new MongoAggregationBuilder(search, storage).build();
        return mongoTemplate.aggregate(aggregation, storage.getCollection(), Document.class).getMappedResults().iterator();

    }

    protected void accumulate(Document document, MongoStorage storage, Query query, Map<String, Accumulator> nameToAccumulator) {
//         year,month,day,hour,minute, dimension... ,metrics...
        if (!Granularity.isAllGranularity(query.getGranularity())) {
            LocalDateTime time = LocalDateTime.of(document.getInteger(YEAR), 1, 1, 0, 0);

            if (query.getGranularity() == Granularity.GMin || query.getGranularity() == Granularity.G5min) {
                time = time.withMonth(document.getInteger(MONTH))
                        .withDayOfMonth(document.getInteger(DAY))
                        .withHour(document.getInteger(HOUR))
                        .withMinute(document.getInteger(MINUTE));
            } else if (query.getGranularity() == Granularity.GHour) {
                time = time.withMonth(document.getInteger(MONTH))
                        .withDayOfMonth(document.getInteger(DAY))
                        .withHour(document.getInteger(HOUR));
            } else if (query.getGranularity() == Granularity.GDay) {
                time = time.withMonth(document.getInteger(MONTH))
                        .withDayOfMonth(document.getInteger(DAY));
            } else if (query.getGranularity() == Granularity.GWeek) {
                WeekFields weekFields = WeekFields.of(Locale.getDefault());
                time = time.with(weekFields.weekOfYear(), document.getInteger(WEEK))
                        .with(weekFields.dayOfWeek(), 1);
            } else if (query.getGranularity() == Granularity.GMonth) {
                time = time.withMonth(document.getInteger(MONTH));
            } else if (query.getGranularity() == Granularity.GQuarter) {
                int quarter = (document.getInteger(MONTH) - 1) / 3 + 1;
                int quarterMonth = (quarter - 1) * 3 + 1;
                time = time.withMonth(quarterMonth);
            }
            String eventTimeField = storage.getEventTimeField();
            nameToAccumulator.get(eventTimeField).add(Timestamp.valueOf(time));
        }

        for (String dimension : query.getDimensions()) {
            nameToAccumulator.get(dimension).add(document.get(dimension));
        }

        for (String metric : query.getMetrics()) {
            nameToAccumulator.get(metric).add(document.get(metric));
        }
    }


    private static class MongoAggregationBuilder {

        private final Query search;
        private final MongoStorage storage;

        protected MongoAggregationBuilder(Query search, MongoStorage storage) {
            this.search = search;
            this.storage = storage;
        }

        public Aggregation build() {
            List<AggregationOperation> pipeline = new ArrayList<>();
            pipeline.add(matchOf(search));
            pipeline.addAll(groupOf(search));
            return Aggregation.newAggregation(pipeline.toArray(new AggregationOperation[0]));
        }

        public MatchOperation matchOf(Query search) {
            String timeRangeField = storage.isCompact() ? storage.getCompactTimeField() : storage.getEventTimeField();
            List<Criteria> criteriaList = new ArrayList<>();
            Criteria timeFilter = where(timeRangeField)
                    .gte(new Date(Timestamp.valueOf(search.getFrom()).getTime()))
                    .lt(new Date(Timestamp.valueOf(search.getTo()).getTime()));
            criteriaList.add(timeFilter);

            if (StringUtils.isNotEmpty(storage.getFilter())) {
                String filterExpr = TemplateUtil.render(storage.getFilter(), search.templateParams());
                Optional<Criteria> criteria = FilterCompiler.compile(filterExpr)
                        .map(it -> it.accept(new MongoCriteriaVisitor()));
                criteria.ifPresent(criteriaList::add);
            }
            if (criteriaList.size() == 1) {
                return Aggregation.match(criteriaList.get(0));
            } else {
                return Aggregation.match(new Criteria().andOperator(criteriaList.toArray(new Criteria[0])));
            }
        }

        private List<AggregationOperation> groupOf(Query search) {
            List<AggregationOperation> result = new ArrayList<>();

            List<String> projections = new ArrayList<>();

            if (!Granularity.isAllGranularity(search.getGranularity())) {
                projections.add(storage.getEventTimeField());

            }
            if (search.getDimensions() != null) {
                projections.addAll(search.getDimensions());
            }
            if (search.getMetrics() != null) {
                projections.addAll(search.getMetrics());
            }

            ProjectionOperation project = Aggregation.project(projections.toArray(new String[0]));

            //如果合并字段，需要对时间和指标字段unwind， 可以拆成一对多
            if (storage.isCompact()) {
                if (!Granularity.isAllGranularity(search.getGranularity())) {
                    UnwindOperation unwindTimes = Aggregation.unwind(storage.getEventTimeField());
                    result.add(unwindTimes);
                }

                for (String metric : search.getMetrics()) {
                    UnwindOperation unwindMetric = Aggregation.unwind(metric);
                    result.add(unwindMetric);
                }
            }

            //day(eventTime),month(eventTime)
            if (!Granularity.isAllGranularity(search.getGranularity())) {
                String eventTimeField = storage.getEventTimeField();
                if (storage.getTimeZone() != null) {
                    DateOperators.Timezone timezone = DateOperators.Timezone.fromZone(TimeZone.getTimeZone(storage.getTimeZone()));
                    DateOperators.DateOperatorFactory timeZoneEventTime = DateOperators.dateOf(eventTimeField).withTimezone(timezone);
                    project = project.and(eventTimeField).as(eventTimeField)
                            .and(timeZoneEventTime.minute()).as(MINUTE)
                            .and(timeZoneEventTime.hour()).as(HOUR)
                            .and(timeZoneEventTime.dayOfMonth()).as(DAY)
                            .and(timeZoneEventTime.week()).as(WEEK)
                            .and(timeZoneEventTime.month()).as(MONTH)
                            .and(timeZoneEventTime.year()).as(YEAR);
                } else {
                    ArithmeticOperators.Add eventTimeAdd8Hours = ArithmeticOperators.Add.valueOf(eventTimeField).add(TimeUnit.HOURS.toMillis(8));
                    project = project.and(eventTimeField).as(eventTimeField)
                            .and(DateOperators.dateValue(eventTimeAdd8Hours).minute()).as(MINUTE)
                            .and(DateOperators.dateValue(eventTimeAdd8Hours).hour()).as(HOUR)
                            .and(DateOperators.dateValue(eventTimeAdd8Hours).dayOfMonth()).as(DAY)
                            .and(DateOperators.dateValue(eventTimeAdd8Hours).week()).as(WEEK)
                            .and(DateOperators.dateValue(eventTimeAdd8Hours).month()).as(MONTH)
                            .and(DateOperators.dateValue(eventTimeAdd8Hours).year()).as(YEAR);
                }

            }
            result.add(project);


            //第二步，做groupBy操作

            List<String> groups = new ArrayList<>();
            if (search.getDimensions() != null && !search.getDimensions().isEmpty()) {
                groups.addAll(search.getDimensions());
            }

            if (Granularity.G5min == search.getGranularity()) {
                groups.add(YEAR);
                groups.add(MONTH);
                groups.add(DAY);
                groups.add(HOUR);
                groups.add(MINUTE);
            } else if (Granularity.GHour == search.getGranularity()) {
                groups.add(YEAR);
                groups.add(MONTH);
                groups.add(DAY);
                groups.add(HOUR);
            } else if (Granularity.GDay == search.getGranularity()) {
                groups.add(YEAR);
                groups.add(MONTH);
                groups.add(DAY);
            } else if (Granularity.GWeek == search.getGranularity()) {
                groups.add(YEAR);
                groups.add(WEEK);
            } else if (Granularity.GMonth == search.getGranularity() || Granularity.GQuarter == search.getGranularity()) {
                groups.add(YEAR);
                groups.add(MONTH);
            } else if (Granularity.GYear == search.getGranularity()) {
                groups.add(YEAR);
            }

            GroupOperation groupOperation = Aggregation.group(groups.toArray(new String[0]));

            //使用first表达式, 输出分组字段
            for (String group : groups) {
                groupOperation = groupOperation.first(group).as(group);
            }

            //指标聚合
            for (String metric : search.getMetrics()) {
                groupOperation = groupOperation.sum(metric).as(metric);
            }
            result.add(groupOperation);
            return result;
        }

    }


    protected Schema buildSchema(MongoStorage storage, Query query) {
        Schema schema = storage.schema();
        List<Field> fields = new ArrayList<>();
        if (!Granularity.isAllGranularity(query.getGranularity())) {
            fields.add(Field.$timestamp(storage.getEventTimeField()));
        }
        for (String dimension : query.getDimensions()) {
            fields.add(schema.getCheckedField(dimension));
        }
        for (String metric : query.getMetrics()) {
            fields.add(schema.getCheckedField(metric));
        }
        return new Schema(fields);
    }

}
