package io.statd.core.storage.jdbc;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.Index;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import io.statd.core.dflib.AccumulatorFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class JdbcUtil {
    private static Index createIndex(SqlRowSet rs) {
        SqlRowSetMetaData rsmd = rs.getMetaData();
        int width = rsmd.getColumnCount();
        String[] names = new String[width];

        for (int i = 0; i < width; i++) {
            names[i] = rsmd.getColumnLabel(i + 1);
        }
        return Index.forLabels(names);
    }

    private static Map<String, Accumulator> createAccummulators(SqlRowSet resultSet) {
        SqlRowSetMetaData rsmd = resultSet.getMetaData();
        int w = rsmd.getColumnCount();
        Map<String, Accumulator> accums = new HashMap<>(w);
        for (int jdbcPos = 1; jdbcPos <= w; jdbcPos++) {
            accums.put(rsmd.getColumnLabel(jdbcPos), AccumulatorFactory.get(rsmd.getColumnType(jdbcPos)));
        }

        return accums;
    }

    public static DataFrame toDataFrame(SqlRowSet sqlRowSet, BiFunction<String, Object, Object> valueConverter) {
        Index index = createIndex(sqlRowSet);
        Map<String, Accumulator> accummulators = createAccummulators(sqlRowSet);

        while ((sqlRowSet.next())) {
            for (String column : index) {
                Object columnValue = sqlRowSet.getObject(column);
                if (columnValue instanceof LocalDateTime) {  //对于LocalDateTime需要转为Timestamp
                    columnValue = Timestamp.valueOf((LocalDateTime) columnValue);
                } else {
                    if (valueConverter != null) {
                        columnValue = valueConverter.apply(column, columnValue);
                    }
                }
                accummulators.get(column).add(columnValue);
            }
        }
        List<Series> series = new ArrayList<>();
        for (String s : index) {
            series.add(accummulators.get(s).toSeries());
        }
        return DataFrame.newFrame(index).columns(series.toArray(new Series[0]));
    }
}
