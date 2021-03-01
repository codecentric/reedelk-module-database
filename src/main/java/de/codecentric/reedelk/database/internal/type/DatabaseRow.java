package de.codecentric.reedelk.database.internal.type;

import de.codecentric.reedelk.runtime.api.annotation.Type;
import de.codecentric.reedelk.runtime.api.annotation.TypeFunction;

import java.io.Serializable;
import java.util.*;

@Type(displayName = "DatabaseRow", mapKeyType = String.class, mapValueType = Serializable.class)
public class DatabaseRow extends HashMap<String, Serializable> {

    private final List<Serializable> values;
    private final Map<String,Integer> columnNameIndexMap;
    private final Map<Integer,String> columnIndexNameMap;

    public DatabaseRow(Map<String,Integer> columnNameIndexMap,
                       Map<Integer,String> columnIndexNameMap,
                       List<Serializable> values) {
        this.columnIndexNameMap = columnIndexNameMap;
        this.columnNameIndexMap = columnNameIndexMap;
        this.values = values;
    }

    @TypeFunction(returnType = Integer.class, signature = "getColumnIndex(String columnName)", cursorOffset = 1)
    public Integer getColumnIndex(String columnName) {
        return columnNameIndexMap.get(columnName);
    }

    @TypeFunction(returnType = String.class, signature = "getColumnName(int columnIndex)", cursorOffset = 1)
    public String getColumnName(int columnIndex) {
        return columnIndexNameMap.get(columnIndex);
    }

    @TypeFunction(returnType = Serializable.class, signature = "get(int columnIndex)", cursorOffset = 1)
    public Serializable get(int columnIndex) {
        return values.get(columnIndex);
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean isEmpty() {
        return values.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return columnNameIndexMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return values.contains(value);
    }

    @Override
    public Serializable get(Object key) {
        Integer valueIndex = columnNameIndexMap.get(key);
        return values.get(valueIndex);
    }

    @Override
    public Serializable put(String key, Serializable value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Serializable remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends String, ? extends Serializable> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> keySet() {
        return columnNameIndexMap.keySet();
    }

    @Override
    public Collection<Serializable> values() {
        return values;
    }

    @Override
    public Set<Entry<String, Serializable>> entrySet() {
        Map<String, Serializable> map = new HashMap<>();
        for (Entry<String,Integer> entry : columnNameIndexMap.entrySet()) {
            String columnName = entry.getKey();
            Integer columnIndex = entry.getValue();
            map.put(columnName, values.get(columnIndex));
        }
        return map.entrySet();
    }
}
