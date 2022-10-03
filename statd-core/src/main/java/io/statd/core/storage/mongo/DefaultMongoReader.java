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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Slf4j
@Component
@Lazy
public class DefaultMongoReader extends MongoReader {

    @Autowired
    public DefaultMongoReader(DataSourceFactory dataSourceFactory) {
        super(dataSourceFactory);
    }


    @Override
    public Table read(Query search, MongoStorage storage) throws StatdException {
        Table table = super.read(search, storage);
        table.setGranularity(Granularity.G5min);
        return table;
    }

    @Override
    protected Iterator<Document> fetchDocuments(Query search, MongoStorage storage, MongoTemplate mongoTemplate) {
        org.springframework.data.mongodb.core.query.Query query = new org.springframework.data.mongodb.core.query.Query();
        String timeRangeField = storage.isCompact() ? storage.getCompactTimeField() : storage.getEventTimeField();
        Criteria criteria = new Criteria(timeRangeField)
                .gte(search.getFrom())
                .lt(search.getTo());
        //filter
        if (StringUtils.isNotEmpty(storage.getFilter())) {
            String filterExpr = TemplateUtil.render(storage.getFilter(), search.templateParams());
            Optional<Criteria> filter = FilterCompiler.compile(filterExpr)
                    .map(it -> it.accept(new MongoCriteriaVisitor()));
            if (filter.isPresent()) {
                criteria = new Criteria().andOperator(criteria, filter.get());
            }
        }
        query.addCriteria(criteria);
        return mongoTemplate.getCollection(storage.getCollection()).find(query.getQueryObject()).iterator();

    }

    @Override
    protected void accumulate(Document document, MongoStorage storage, Query query, Map<String, Accumulator> nameToAccumulator) {
        Schema schema = buildSchema(storage, query);
        if (storage.isCompact()) {
            //5分钟点的时
            List<Date> eventTimeList = (List<Date>) document.get(storage.getEventTimeField());
            Map<String, List<Long>> metrics = new HashMap<>();
            for (Field metric : storage.metricFields()) {
                metrics.put(metric.getName(), (List<Long>) document.get(metric.getName()));
            }

            for (int row = 0; row < eventTimeList.size(); row++) {
                for (Field field : schema.getFields()) {
                    if (field.getName().equals(storage.getEventTimeField())) {
                        Date eventTime = eventTimeList.get(row);
                        nameToAccumulator.get(field.getName()).add(new Timestamp(eventTime.getTime()));
                    } else if (query.getMetrics().contains(field.getName())) {
                        nameToAccumulator.get(field.getName()).add(metrics.get(field.getName()).get(row));
                    } else {
                        nameToAccumulator.get(field.getName()).add(document.get(field.getName()));
                    }
                }
            }
        } else {
            for (Field field : schema.getFields()) {
                if (field.getName() == storage.getEventTimeField()) {
                    Date date = document.getDate(field.getName());
                    nameToAccumulator.get(field.getName()).add(new Timestamp(date.getTime()));
                } else {
                    nameToAccumulator.get(field.getName()).add(document.get(field.getName()));
                }

            }
        }

    }

    protected Schema buildSchema(MongoStorage storage, Query query) {
        Schema schema = storage.schema();
        List<Field> fields = new ArrayList<>();
        fields.add(Field.$timestamp(storage.getEventTimeField()));
        for (String dimension : query.getDimensions()) {
            fields.add(schema.getCheckedField(dimension));
        }
        for (String metric : query.getMetrics()) {
            fields.add(schema.getCheckedField(metric));
        }
        return new Schema(fields);
    }
}
