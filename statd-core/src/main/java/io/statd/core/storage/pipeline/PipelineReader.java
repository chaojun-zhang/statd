package io.statd.core.storage.pipeline;

import io.statd.core.dataframe.Catalog;
import io.statd.core.dataframe.Table;
import io.statd.core.dataframe.TimeSeries;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Query;
import io.statd.core.storage.DataFrameLoader;
import io.statd.core.storage.StorageReader;
import io.statd.core.storage.config.PipelineStorage;
import io.statd.core.storage.pipeline.calcite.CalciteQueryEngine;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

@Lazy
@Component
public class PipelineReader implements StorageReader<PipelineStorage> {

    private final DataFrameLoader datasetReader;

    @Autowired
    public PipelineReader(DataFrameLoader datasetReader) {
        this.datasetReader = datasetReader;
    }

    @Override
    public Table read(Query query, PipelineStorage storage) throws StatdException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Table> readMore(Query query, PipelineStorage pipelineStorage) {
        try (Catalog catalog = new Catalog()) {
            catalog.setFunctions(pipelineStorage.getFunctions());
            catalog.setParams(pipelineStorage.getParams());
            load(catalog, query, pipelineStorage);
            transform(query, pipelineStorage, catalog);
            return output(query, pipelineStorage, catalog);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, Table> output(Query query, PipelineStorage pipelineStorage, Catalog catalog) {
        final Map<String, Table> result = new HashMap<>();
        for (PipelineStorage.Output output : pipelineStorage.getOutput()) {
            Table table = catalog.findTable(output.getDataFrameName());
            if (output.isFillZero() || StringUtils.isNotEmpty(output.getFillZeroByField())) {
                table = new TimeSeries(table, query.getInterval(), output.getFillZeroByField(), query.getGranularity());
                result.put(output.getName(), table);
            } else {
                result.put(output.getName(), table);
            }
        }
        return result;
    }

    private void transform(Query query, PipelineStorage pipelineStorage, Catalog catalog) {

        if (StringUtils.isNotEmpty(pipelineStorage.getTransform())) {
            try (CalciteQueryEngine calciteQueryEngine = new CalciteQueryEngine(pipelineStorage, catalog, query)) {
                calciteQueryEngine.execute(pipelineStorage.getTransform());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void load(Catalog catalog, Query query, PipelineStorage pipelineStorage) {
        pipelineStorage.getInput().entrySet().stream().parallel()
                .forEach(nameToStorage -> {
                    Table table = datasetReader.load(query, nameToStorage.getValue());
                    catalog.registerTable(nameToStorage.getKey(), table);
                });
    }
}
