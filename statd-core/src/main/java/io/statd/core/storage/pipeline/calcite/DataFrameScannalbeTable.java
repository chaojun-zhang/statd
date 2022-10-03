package io.statd.core.storage.pipeline.calcite;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.row.RowProxy;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;

import java.sql.Timestamp;
import java.util.Iterator;

public class DataFrameScannalbeTable extends DataFrameTable implements ScannableTable {
    public DataFrameScannalbeTable(DataFrame dataFrame) {
        super(dataFrame);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>() {
                    private Iterator<RowProxy> iterator = dataFrame.iterator();
                    Object[] current;
    
                    @Override
                    public Object[] current() {
                        if (current == null) {
                            this.moveNext();
                        }
                        return current;
                    }
    
                    @Override
                    public boolean moveNext() {
                        try {
                            if (this.iterator.hasNext()) {
                                RowProxy next = iterator.next();
                                int columnWidth = next.getIndex().size();
                                current = new Object[columnWidth];
                                for (int col = 0; col < columnWidth; col++) {
                                    Object columnValue = next.get(col);
                                    if (columnValue instanceof Timestamp) {
                                        current[col] = ((Timestamp) columnValue).getTime();
                                    } else {
                                        current[col] = columnValue;
                                    }
                                }
                                return true;
                            } else {
                                current = null;
                                return false;
                            }
                        } catch (RuntimeException | Error e) {
                            throw e;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    @Override
                    public void reset() {
                        iterator = dataFrame.iterator();
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        };
    }


}
