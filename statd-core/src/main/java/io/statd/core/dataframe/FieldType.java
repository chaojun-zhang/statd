package io.statd.core.dataframe;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;

public enum FieldType {
    INT(Integer.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    FLOAT(Float.class),
    TIMESTAMP(Timestamp.class),
    STRING(String.class),
    BOOLEAN(Boolean.class);

    public Class clazz;

    FieldType(Class clazz) {
        this.clazz = clazz;
    }

    public static Optional<FieldType> of(String type) {
        return Arrays.stream(FieldType.values()).filter(it -> it.name().equalsIgnoreCase(type)).findFirst();
    }


    public Object parseValue(Object object) {
        Object value = object;
        switch (this) {
            case DOUBLE:
                if (value == null) {
                    value = 0.;
                } else if (value.getClass() == Integer.class) {
                    value = ((Integer) value).doubleValue();
                } else if (value.getClass() == Long.class) {
                    value = ((Long) value).doubleValue();
                } else if (value.getClass() == String.class) {
                    value = Double.parseDouble(value.toString());
                } else if (value.getClass() == Float.class) {
                    value = ((Float) value).doubleValue();
                }
                break;
            case FLOAT:
                if (value == null) {
                    value = 0.f;
                } else if (value.getClass() == Integer.class) {
                    value = ((Integer) value).floatValue();
                } else if (value.getClass() == Long.class) {
                    value = ((Long) value).floatValue();
                } else if (value.getClass() == String.class) {
                    value = FLOAT.parseValue(value.toString());
                }else if (value.getClass() == Double.class) {
                    value = ((Double) value).floatValue();
                }
                break;
            case LONG:
                if (value == null) {
                    value = 0L;
                } else if (value.getClass() == Integer.class) {
                    value = ((Integer) value).longValue();
                } else if (value.getClass() == Double.class) {
                    value = ((Double) value).longValue();
                } else if (value.getClass() == Float.class) {
                    value = ((Float) value).longValue();
                }  else if (value.getClass() == String.class) {
                    value = Long.parseLong(value.toString());
                }
                break;
            case INT:
                if (value == null) {
                    value = 0;
                } else if (value.getClass() == Long.class) {
                    value = ((Long) value).intValue();
                } else if (value.getClass() == Double.class) {
                    value = ((Double) value).intValue();
                } else if (value.getClass() == String.class) {
                    value = Integer.parseInt(value.toString());
                }else if (value.getClass() == Float.class) {
                    value = ((Float) value).intValue();
                }
                break;
            case TIMESTAMP:
                if (value == null) {
                    throw new NullPointerException("timestamp cannot be null");
                } else if (Integer.class == value.getClass()) {
                    value = new Timestamp(1000L * (int) value);
                } else if (Long.class == value.getClass()) {
                    value = new Timestamp((long) value);
                }
                break;
            default:
                break;
        }
        return value;
    }

}
