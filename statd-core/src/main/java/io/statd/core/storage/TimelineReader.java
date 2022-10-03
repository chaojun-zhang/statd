package io.statd.core.storage;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.DataFrameBuilder;
import com.nhl.dflib.DataFrameByRowBuilder;
import com.nhl.dflib.row.RowProxy;
import io.statd.core.dataframe.Table;
import io.statd.core.exception.QueryRequestError;
import io.statd.core.exception.StatdException;
import io.statd.core.query.Granularity;
import io.statd.core.query.Query;
import io.statd.core.storage.config.TimelineStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Lazy
@Component
public class TimelineReader implements StorageReader<TimelineStorage> {

    private final DataFrameLoader datasetReader;

    @Autowired
    public TimelineReader(DataFrameLoader datasetReader) {
        this.datasetReader = datasetReader;
    }

    @Override
    public Table read(Query query, TimelineStorage storage) throws StatdException {
        if (storage.getBatch() != null && storage.getStream() == null) {
            return datasetReader.load(query, storage.getBatch());
        } else if (storage.getBatch() == null && storage.getStream() != null) {
            return datasetReader.load(query, storage.getStream());
        } else {
            if (Granularity.isAllGranularity(query.getGranularity())) {
                throw new StatdException(QueryRequestError.InvalidGranule, "query granularity is required");
            }

            Table batchDataFrame = datasetReader.load(query, storage.getBatch());
            Table streamBatchFrame = datasetReader.load(query, storage.getStream());
            if (!batchDataFrame.getSchema().isSameIgnoreTimestamp(streamBatchFrame.getSchema())) {
                throw new IllegalStateException("schema are not same between  batch and stream datasource");
            }
            if (batchDataFrame.getGranularity() != streamBatchFrame.getGranularity()) {
                throw new IllegalStateException("granularity are not same between batch and stream datasource");
            }

            Table table = merge(batchDataFrame, streamBatchFrame);
            table.setGranularity(query.getGranularity());
            return table;
        }
    }

    /**
     * 排序后进行合并
     */
    private Table merge(Table batch, Table stream) {
        if (batch.getDataFrame().height() == 0) {
            return stream;
        }
        if (stream.getDataFrame().height() == 0) {
            return batch;
        }

        final DataFrame sortedBatch = batch.getDataFrame().sort(batch.getSchema().getEventTimeField().get().getName(), true);
        final DataFrame sortedStream = stream.getDataFrame().sort(stream.getSchema().getEventTimeField().get().getName(), true);

        final java.util.Iterator<RowProxy> batchIterator = sortedBatch.iterator();
        final java.util.Iterator<RowProxy> streamIterator = sortedStream.iterator();

        RowProxy batchRow = batchIterator.next();
        RowProxy streamRow = streamIterator.next();

        DataFrameByRowBuilder dataFrameByRowBuilder = DataFrameBuilder.builder(batch.getDataFrame().getColumnsIndex()).byRow(batch.getSchema().accumulators());
        while (batchRow != null || streamRow != null) {
            if (streamRow == null) {
                dataFrameByRowBuilder.addRow(toRow(batchRow));
                batchRow = batchIterator.hasNext() ? batchIterator.next() : null;
            } else if (batchRow == null) {
                dataFrameByRowBuilder.addRow(toRow(streamRow));
                streamRow = streamIterator.hasNext() ? streamIterator.next() : null;
            } else {
                Timestamp batchTimestamp = (Timestamp) batchRow.get(batch.getSchema().getEventTimeField().get().getName());
                Timestamp streamTimestamp = (Timestamp) streamRow.get(stream.getSchema().getEventTimeField().get().getName());
                if (batchTimestamp.getTime() > streamTimestamp.getTime()) {
                    dataFrameByRowBuilder.addRow(toRow(streamRow));
                    streamRow = streamIterator.hasNext() ? streamIterator.next() : null;
                } else if (batchTimestamp.getTime() < streamTimestamp.getTime()) {
                    dataFrameByRowBuilder.addRow(toRow(batchRow));
                    batchRow = batchIterator.hasNext() ? batchIterator.next() : null;
                } else {
                    dataFrameByRowBuilder.addRow(toRow(batchRow));
                    streamRow = streamIterator.hasNext() ? streamIterator.next() : null;
                    batchRow = batchIterator.hasNext() ? batchIterator.next() : null;
                }
            }
        }
        return Table.create(dataFrameByRowBuilder.create());
    }

    private Object[] toRow(RowProxy rowProxy) {
        Object[] row = new Object[rowProxy.getIndex().size()];
        for (int i = 0; i < rowProxy.getIndex().size(); i++) {
            row[i] = rowProxy.get(i);
        }
        return row;
    }


}