package com.reedelk.database.internal.commons;

import com.reedelk.runtime.api.message.content.DataRow;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class JDBCDataRow implements DataRow<Serializable> {

    private final JDBCMetadata attributes;
    private final List<Serializable> row;

    JDBCDataRow(JDBCMetadata attributes, List<Serializable> row) {
        this.attributes = attributes;
        this.row = Collections.unmodifiableList(row);
    }

    @Override
    public Map<String, Serializable> attributes() {
        return attributes;
    }

    @Override
    public Serializable attribute(String name) {
        return attributes.get(name);
    }

    @Override
    public int columnCount() {
        return attributes.getColumnCount();
    }

    @Override
    public String columnName(int i) {
        return attributes.getColumnName(i);
    }

    @Override
    public List<String> columnNames() {
        return attributes.getColumnNames();
    }

    @Override
    public Serializable get(int column) {
        return row.get(column);
    }

    @Override
    public Serializable getByColumnName(String columnName) {
        int index = attributes.getColumnIndex(columnName);
        return row.get(index);
    }

    @Override
    public List<Serializable> values() {
        return row;
    }

    @Override
    public String toString() {
        return "JDBCDataRow{" +
                "columnNames=" + attributes.getColumnNames() +
                ", row=" + row +
                ", attributes=" + attributes +
                '}';
    }
}
