package io.statd.core.storage.spark;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import io.statd.core.dataframe.FieldType;
import io.statd.core.dataframe.Table;
import io.statd.core.dflib.AccumulatorFactory;
import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.config.SparkStorage;
import io.statd.core.storage.jdbc.SqlRender;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
@Lazy
public class SparkHttpReader implements StorageReader<SparkStorage> {

    private final LoadingCache<String, Retrofit> retrofitCache = Caffeine.newBuilder()
            .build(this::retrofit);

    @Override
    public Table read(Query query, SparkStorage storage) throws StatdException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Table> readMore(Query query, SparkStorage storage) throws StatdException {
        if (query.getInterval().days().size() > storage.getMaxQueryDays()) {
            throw new StatdException(QueryRequestError.InvalidInterval, "Query time range cannot exceed " + storage.getMaxQueryDays() + " days");
        }


        SqlRender sqlParser = new SqlRender(query, storage);
        String sql = sqlParser.sql();
        try {
            Retrofit retrofit = retrofitCache.get(storage.getBaseUrl());
            SparkJobClient sparkJobClient = retrofit.create(SparkJobClient.class);

            //构造spark pipeline 请求对象
            PipelineRequest request = new PipelineRequest();
            request.setOutputPrefix(sqlParser.getViewPrefix());
            request.setOutputs(storage.getOutputs());
            request.setPipeline(sql);
            request.setLimit(storage.getRowLimit());

            Response<PipelineResponse> execute = sparkJobClient.pipeline(request).execute();
            if (execute.isSuccessful()) {
                PipelineResponse response = execute.body();
                if (response != null) {
                    return buildTables(query, response, storage);
                }
            }
            String errorMessage = String.format("fail to execute pipeline, sql: %s, queryId: %s, query: %s, httpCode: %s, error: %s",
                    sql, query.getQueryId(), query, execute.code(), execute.errorBody());
            throw new RuntimeException(errorMessage);
        } catch (IOException e) {
            String errorMessage = String.format("fail to execute pipeline, sql: %s, queryId: %s, query: %s", sql, query.getQueryId(), query);
            throw new UncheckedIOException(errorMessage, e);
        }
    }

    private Retrofit retrofit(String url) {
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(20);
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder.dispatcher(dispatcher);
        builder.readTimeout(10, TimeUnit.MINUTES);
        builder.writeTimeout(10, TimeUnit.MINUTES);
        builder.connectTimeout(1, TimeUnit.MINUTES);

        builder.connectionPool(new ConnectionPool(16, 30, TimeUnit.SECONDS));

        OkHttpClient okHttpClient = builder.build();
        return new Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(JacksonConverterFactory.create())
                .client(okHttpClient)
                .build();
    }

    private Map<String, Table> buildTables(Query query, PipelineResponse sparkPipelineResponse, SparkStorage storage) {
        Map<String, Table> result = new HashMap<>();
        Map<String, PipelineResultTable> tables = sparkPipelineResponse.getTables();

        tables.forEach((name, table) -> {
            Table dfTable = Table.create(parseDataFrame(table, storage));
            dfTable.setGranularity(query.getGranularity());
            result.put(name, dfTable);
        });
        return result;

    }

    private DataFrame parseDataFrame(PipelineResultTable table, SparkStorage storage) {
        String[] fields = table.getSchema().stream().map(PipelineResultField::getName).toArray(String[]::new);
        Index index = Index.forLabels(fields);

        Map<String, FieldType> schema = new HashMap<>();
        for (PipelineResultField pipelineResultField : table.getSchema()) {
            Optional<FieldType> fieldType = FieldType.of(pipelineResultField.getType());
            fieldType.ifPresent(type -> schema.put(pipelineResultField.getName(), type));
        }

        Map<String, Accumulator> accummulators = new HashMap<>();
        for (PipelineResultField field : table.getSchema()) {
            accummulators.put(field.getName(), AccumulatorFactory.get(FieldType.valueOf(field.getType().toUpperCase())));
        }

        //TODO 改成用avro格式进行调用
        for (Map<String, Object> row : table.getRows()) {
            for (String column : index) {
                Object value = schema.get(column).parseValue(row.get(column));
                accummulators.get(column).add(value);
            }
        }

        List<Series> series = new ArrayList<>();
        for (String s : index) {
            series.add(accummulators.get(s).toSeries());
        }
        return DataFrame.newFrame(index).columns(series.toArray(new Series[0]));
    }
}
