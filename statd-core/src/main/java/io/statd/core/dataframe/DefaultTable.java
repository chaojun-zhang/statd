package io.statd.core.dataframe;

import com.nhl.dflib.DataFrame;
import io.statd.core.query.Granularity;
import lombok.Getter;
import lombok.Setter;


public class DefaultTable implements Table {

    @Getter
    private final DataFrame dataFrame;

    @Getter
    private final Schema schema;

    @Getter
    @Setter
    private Granularity granularity;

    protected DefaultTable(DataFrame dataFrame) {
        this.dataFrame = dataFrame;
        this.schema = Schema.createFromDataFrame(dataFrame);
    }
}
