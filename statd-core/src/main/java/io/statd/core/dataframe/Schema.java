package io.statd.core.dataframe;

import com.nhl.dflib.DataFrame;
import com.nhl.dflib.DoubleSeries;
import com.nhl.dflib.FloatSeries;
import com.nhl.dflib.IntSeries;
import com.nhl.dflib.LongSeries;
import com.nhl.dflib.Series;
import com.nhl.dflib.accumulator.Accumulator;
import com.nhl.dflib.series.TimestampSeries;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Data
public final class Schema {
    private final List<Field> fields;
    private final Map<String, Integer> nameToIndex;
    private final Map<String, Field> nameToField;

    public static Schema of(Field... fields) {
        return new Schema(Arrays.asList(fields));
    }

    public Schema(List<Field> fields) {
        this.fields = Objects.requireNonNull(fields);
        this.nameToField = new HashMap<>();
        this.nameToIndex = new HashMap<>();
        int index = 0;
        for (Field field : fields) {
            nameToIndex.put(field.getName(), index++);
            nameToField.put(field.getName(), field);
        }
    }

    public boolean isSame(Schema schema){
        return Arrays.equals(fields.toArray(),schema.fields.toArray());
    }

    public boolean isSameIgnoreTimestamp(Schema schema) {
        if (fields.size() != schema.fields.size()) {
            return false;
        }
        Object[] thisFields = this.fields.stream().filter(it -> it.getType() != FieldType.TIMESTAMP).toArray();
        Object[] thatFields = schema.fields.stream().filter(it -> it.getType() != FieldType.TIMESTAMP).toArray();
        return Arrays.equals(thisFields, thatFields);
    }


    public Optional<Field> getEventTimeField() {
        return fields.stream().filter(it -> it.getType() == FieldType.TIMESTAMP).findFirst();
    }

    public Optional<Field> findField(String name) {
        return fields.stream().filter(it -> it.getName().equals(name)).findFirst();
    }

    public Field getCheckedField(String name) {
        return fields.stream().filter(it -> it.getName().equals(name)).findFirst().orElseThrow(() ->
                new IllegalArgumentException("field '" + name + "' not found"));
    }


    public String[] allColumnLabels() {
        return fields.stream().map(Field::getName).toArray(String[]::new);
    }

    public Accumulator[] accumulators() {
        return fields.stream().map(Field::accumulator).toArray(Accumulator[]::new);
    }

    /*
    fields的顺序应该保证为time,dimension,metric这种顺序，这样构造出的accumulator也保证为这种顺序。与timeseries.toDF中addRow的时候
    输入的数据格式保证一致
     */

    public static Schema createFromDataFrame(DataFrame dataFrame) {
        List<Field> fields = new ArrayList<>();
        for (String columnsIndex : dataFrame.getColumnsIndex()) {
            Series column = dataFrame.getColumn(columnsIndex);
            if (column instanceof TimestampSeries) {
                fields.add(Field.$timestamp(columnsIndex));
            } else if (column instanceof LongSeries) {
                fields.add(Field.$long(columnsIndex));
            } else if (column instanceof DoubleSeries) {
                fields.add(Field.$double(columnsIndex));
            }  else if (column instanceof FloatSeries) {
                fields.add(Field.$float(columnsIndex));
            }else if (column instanceof IntSeries) {
                fields.add(Field.$int(columnsIndex));
            } else {
                fields.add(Field.$str(columnsIndex));
            }
        }
        return new Schema(fields);
    }


}
