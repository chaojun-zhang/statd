package io.statd.core.dflib;

import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.accumulator.BooleanAccumulator;
import com.nhl.dflib.accumulator.DoubleAccumulator;
import com.nhl.dflib.accumulator.FloatAccumulator;
import com.nhl.dflib.accumulator.IntAccumulator;
import com.nhl.dflib.accumulator.LongAccumulator;
import com.nhl.dflib.accumulator.ObjectAccumulator;
import com.nhl.dflib.accumulator.StringAccumulator;
import com.nhl.dflib.accumulator.TimestampAccumulator;
import io.statd.core.dataframe.FieldType;

import java.sql.Types;

public interface AccumulatorFactory {

    static Accumulator<?> get(Class<?> type) {
        return get(type, 10);
    }

    static Accumulator<?> get(Class<?> type, int capacity) {
        return Accumulator.factory(type, capacity);
    }

    static Accumulator<?> get(FieldType type) {
        return Accumulator.factory(type.clazz, 10);
    }

    static Accumulator<?> get(int jdbcType) {
        switch (jdbcType) {
            case Types.BOOLEAN:
                return new BooleanAccumulator();
            case Types.INTEGER:
                return new IntAccumulator();
            case Types.DOUBLE:
                return new DoubleAccumulator();
            case Types.FLOAT:
                return new FloatAccumulator();
            case Types.BIGINT:
                return new LongAccumulator();
            case Types.TIMESTAMP:
                return new TimestampAccumulator();
            case Types.BINARY:
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return new StringAccumulator();
            default:
                return new ObjectAccumulator<>();
        }
    }
}
