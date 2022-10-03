package io.statd.core.storage.pipeline.calcite;

import com.nhl.dflib.DataFrame;
import io.statd.core.dataframe.Field;
import io.statd.core.dataframe.FieldType;
import io.statd.core.dataframe.Schema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DataFrameTable extends AbstractTable {

    protected final DataFrame dataFrame;
    protected RelDataType rowType;

    public DataFrameTable(DataFrame dataFrame) {
        this.dataFrame = dataFrame;
    }


    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            rowType = createRelDataType(typeFactory);
        }
        return rowType;
    }

    private RelDataType createRelDataType(RelDataTypeFactory typeFactory) {
        List<RelDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Schema schema = Schema.createFromDataFrame(dataFrame);
        for (Field field : schema.getFields()) {
            DataFrameFieldType type = DataFrameFieldType.of(field.getType());
            RelDataType relDataType = type.toType((JavaTypeFactory) typeFactory);
            types.add(relDataType);
            names.add(field.getName());
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    enum DataFrameFieldType {
        STRING(String.class, FieldType.STRING),
        BOOLEAN(Primitive.BOOLEAN, FieldType.BOOLEAN),
        INT(Primitive.INT, FieldType.INT),
        LONG(Primitive.LONG, FieldType.LONG),
        DOUBLE(Primitive.DOUBLE, FieldType.DOUBLE),
        FLOAT(Primitive.FLOAT, FieldType.FLOAT),
        TIMESTAMP(java.sql.Timestamp.class, FieldType.TIMESTAMP);

        private final Class<?> clazz;
        private final FieldType columnType;

        private static final Map<FieldType, DataFrameFieldType> MAP = new HashMap<>();

        static {
            for (DataFrameFieldType value : values()) {
                MAP.put(value.columnType, value);
            }
        }

        DataFrameFieldType(Primitive primitive, FieldType columnType) {
            this(primitive.boxClass, columnType);
        }

        DataFrameFieldType(Class<?> clazz, FieldType columnType) {
            this.clazz = clazz;
            this.columnType = columnType;
        }

        public RelDataType toType(JavaTypeFactory typeFactory) {
            RelDataType javaType = typeFactory.createJavaType(clazz);
            RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
            return typeFactory.createTypeWithNullability(sqlType, true);
        }

        public static DataFrameFieldType of(FieldType columnType) {
            return MAP.get(columnType);
        }
    }
}
