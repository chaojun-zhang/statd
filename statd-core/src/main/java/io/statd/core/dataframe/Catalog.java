package io.statd.core.dataframe;

import lombok.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Data
public class Catalog implements Closeable {

    private Map<String, String> functions = new HashMap<>();

    private static ThreadLocal<Catalog> currentCatalog = new ThreadLocal<>();

    private Map<String, String> params = new HashMap<>();

    private Map<String, Table> nameToTable = new HashMap<>();

    public Catalog() {
        currentCatalog.set(this);
    }

    public void registerTable(String name, Table table) {
        if(!nameToTable.containsKey(name)){
            nameToTable.put(name, table);
        }
    }

    public Table findTable(String tableName) {
        return nameToTable.get(tableName);
    }

    public String getParam(String key) {
        return this.params.get(key);
    }

    public static Catalog getSession() {
        return currentCatalog.get();
    }

    @Override
    public void close() throws IOException {
        currentCatalog.remove();
    }
}
