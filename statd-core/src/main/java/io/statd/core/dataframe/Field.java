package io.statd.core.dataframe;

import com.nhl.dflib.accumulator.Accumulator;
import io.statd.core.dflib.AccumulatorFactory;
import lombok.Getter;

import java.util.Objects;
import java.util.Optional;

@Getter
public class Field {

    private final String name;
    private final FieldType type;

    public Field(String name, FieldType type) {
        this.name = name;
        this.type = type;
    }

    public static Field createFromString(String fieldAndType) {
        String[] nameType = fieldAndType.split("\\s+");
        if (nameType.length != 2) {
            throw new IllegalArgumentException("invalid field, field must start with fieldName and follow by fieldType, actual:" + fieldAndType);
        }

        String name = nameType[0].trim();
        String type = nameType[1].trim();

        Optional<FieldType> fieldType = FieldType.of(type);
        if (fieldType.isPresent()) {
            return new Field(name, fieldType.get());
        } else {
            throw new IllegalArgumentException("invalid field type, field name:" + name + ", field type: " + type);
        }
    }


    public Accumulator accumulator() {
        return AccumulatorFactory.get(this.type);
    }


    public static Field $int(String name) {
        return new Field(name, FieldType.INT);
    }

    public static Field $long(String name) {
        return new Field(name, FieldType.LONG);
    }

    public static Field $double(String name) {
        return new Field(name, FieldType.DOUBLE);
    }

    public static Field $float(String name) {
        return new Field(name, FieldType.FLOAT);
    }

    public static Field $str(String name) {
        return new Field(name, FieldType.STRING);
    }

    public static Field $timestamp(String name) {
        return new Field(name, FieldType.TIMESTAMP);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field field = (Field) o;
        return Objects.equals(name, field.name) && type == field.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }
}
