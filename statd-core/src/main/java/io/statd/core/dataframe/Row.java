package io.statd.core.dataframe;

import lombok.Getter;

import java.util.Arrays;

public final class Row {
    @Getter
    private final Object[] data;

    public Row(Object[] rowValues) {
        this.data = rowValues;
    }

    public int size() {
        return data.length;
    }

    public Object get(int i) {
        return data[i];
    }


    public boolean isEmpty() {
        return this.data.length == 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        Row that = (Row) obj;
        return Arrays.equals(this.data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }


    public static Row create(Object... values) {
        return new Row(values);
    }

}
