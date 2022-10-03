package io.statd.core.dataframe;

import com.nhl.dflib.DataFrame;
import io.statd.core.query.Granularity;

public interface Table {

    DataFrame getDataFrame();

    Schema getSchema();

    void setGranularity(Granularity granularity);

    Granularity getGranularity();

    static Table create(DataFrame dataFrame) {
        return new DefaultTable(dataFrame);
    }
}
