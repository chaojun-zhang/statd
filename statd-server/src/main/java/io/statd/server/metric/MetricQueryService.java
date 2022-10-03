package io.statd.server.metric;

import com.alibaba.fastjson.JSON;
import com.google.common.io.CountingOutputStream;
import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.csv.Csv;
import com.nhl.dflib.excel.Excel;
import io.statd.core.dataframe.Table;
import io.statd.core.query.Record;
import io.statd.core.storage.DataFrameLoader;
import io.statd.core.storage.config.MultipleStorage;
import io.statd.core.storage.config.SingleStorage;
import io.statd.core.storage.config.Storage;
import io.statd.core.storage.template.TemplateUtil;
import io.statd.server.config.Config;
import io.statd.server.config.StorageConfig;
import io.statd.server.model.MetricQuery;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.util.io.BufferingOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Component
@Slf4j
@CacheConfig(cacheNames = "loadMetric")
public class MetricQueryService {

    private final DataFrameLoader datasetReader;

    private final Config config;

    @Autowired
    public MetricQueryService(DataFrameLoader datasetReader, Config config) {
        this.datasetReader = datasetReader;
        this.config = config;
    }

    @Cacheable
    public Object load(MetricQuery query) {
        StorageConfig storageConfig = config.getStorage(query.getModuleMetric());
        Map<String, Table> tables = loadTables(query, storageConfig);
        Map<String, Object> pipelineResult = new HashMap<>();
        tables.forEach((k, table) -> {
            Index columnsIndex = table.getDataFrame().getColumnsIndex();
            List<Record> rows = new ArrayList<>();
            table.getDataFrame().forEach(row -> {
                Record res = new Record();
                for (String colName : columnsIndex) {
                    Object value = row.get(colName);
                    if (value instanceof Timestamp) {
                        Timestamp timestamp = (Timestamp) value;
                        res.put(colName, timestamp.getTime());
                    } else {
                        res.put(colName, value);
                    }
                }
                rows.add(res);
            });
            pipelineResult.put(k, rows);
        });

        if (StringUtils.isEmpty(storageConfig.getRender())) {
            return pipelineResult;
        } else {
            final Map<String, Object> model = new HashMap<>(pipelineResult);
            model.put("print", new TablePrinter());
            String result = TemplateUtil.render(storageConfig.getRender(), model);
            if (storageConfig.isArray()) {
                return JSON.parseArray(result);
            } else {
                return JSON.parseObject(result);
            }
        }
    }

    public long export(MetricQuery query, OutputStream outputStream, String exportType) {
        StorageConfig storageConfig = config.getStorage(query.getModuleMetric());
        Map<String, Table> tables = this.loadTables(query, storageConfig);
        long length = 0;
        try (ZipOutputStream zos = new ZipOutputStream(outputStream)) {
            for (Map.Entry<String, Table> nameToDF : tables.entrySet()) {
                ZipEntry zipEntry = new ZipEntry(nameToDF.getKey());
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                try (CountingOutputStream os = new CountingOutputStream(new BufferingOutputStream(byteArrayOutputStream))) {
                    if ("excel".equalsIgnoreCase(exportType)) {
                        Map<String, DataFrame> dfs = new HashMap<>();
                        dfs.put(nameToDF.getKey(), nameToDF.getValue().getDataFrame());
                        Excel.saver().save(dfs, os);
                        os.flush();
                    } else {
                        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(new BufferingOutputStream(byteArrayOutputStream));
                        Csv.save(nameToDF.getValue().getDataFrame(), outputStreamWriter);
                        outputStreamWriter.flush();
                    }
                    zipEntry.setSize(os.getCount());
                    zos.putNextEntry(zipEntry);
                    StreamUtils.copy(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()), zos);
                    zos.closeEntry();
                    length += os.getCount();
                }
            }
            zos.finish();
            return length;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Map<String, Table> loadTables(MetricQuery metricQuery, StorageConfig storageConfig) {
        Storage storage = storageConfig.getStorage();
        if (storage instanceof MultipleStorage) {
            return datasetReader.load(metricQuery.getQuery(), (MultipleStorage) storage);
        } else {
            Table dataFrame = datasetReader.load(metricQuery.getQuery(), (SingleStorage) storage);
            return Collections.singletonMap("default", dataFrame);
        }
    }


}
