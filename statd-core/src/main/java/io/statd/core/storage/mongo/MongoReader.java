package io.statd.core.storage.mongo;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import io.statd.core.dataframe.Field;
import io.statd.core.dataframe.Schema;
import io.statd.core.dataframe.Table;
import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import io.statd.core.storage.DataSourceFactory;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.config.MongoStorage;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Slf4j
public abstract class MongoReader implements StorageReader<MongoStorage> {

    protected final DataSourceFactory dataSourceFactory;

    public MongoReader(DataSourceFactory dataSourceFactory) {
        this.dataSourceFactory = dataSourceFactory;
    }

    @Override
    public Table read(Query search, MongoStorage storage) throws StatdException {
        if (search.getInterval().days().size() > storage.getMaxQueryDays()) {
            throw new StatdException(QueryRequestError.InvalidInterval, "Query time range cannot exceed " + storage.getMaxQueryDays() + " days");
        }

        if (search.getDimensions().isEmpty() && storage.dimensionFields() != null
                && !storage.dimensionFields().isEmpty()) {
            List<String> diemensions = storage.dimensionFields().stream().map(Field::getName).collect(Collectors.toList());
            search.setDimensions(diemensions);
        }

        if (search.getMetrics().isEmpty() && storage.metricFields() != null
                && !storage.metricFields().isEmpty()) {
            List<String> metrics = storage.metricFields().stream().map(Field::getName).collect(Collectors.toList());
            search.setMetrics(metrics);
        }

        return loadStorage(search, storage);
    }


    /**
     * 支持紧凑类型的查询后聚合和本地聚合
     */
    private Table loadStorage(Query search, MongoStorage storage) {
        final MongoTemplate mongoTemplate = new MongoTemplate(dataSourceFactory.getMongoDatabaseFactory(storage.getDatasource()));
        final Schema schema = buildSchema(storage, search);

        final Map<String, Accumulator> nameToAccumulator = new HashMap<>();
        schema.getFields().forEach(field -> {
            nameToAccumulator.put(field.getName(), field.accumulator());
        });

        final Iterator<Document> documents = fetchDocuments(search, storage, mongoTemplate);
        documents.forEachRemaining(document -> {
            accumulate(document, storage, search, nameToAccumulator);
        });

        final List<Series> series = new ArrayList<>();
        for (String s : schema.allColumnLabels()) {
            series.add(nameToAccumulator.get(s).toSeries());
        }

        final DataFrame dataFrame = DataFrame.newFrame(schema.allColumnLabels()).columns(series.toArray(new Series[0]));
        return Table.create(dataFrame);
    }

    protected abstract Schema buildSchema(MongoStorage storage, Query query);

    protected abstract Iterator<Document> fetchDocuments(Query search, MongoStorage storage, MongoTemplate mongoTemplate);


    protected abstract void accumulate(Document document, MongoStorage storage, Query query, Map<String, Accumulator> nameToAccumulator);

}
