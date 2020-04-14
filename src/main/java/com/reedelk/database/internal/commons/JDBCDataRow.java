package com.reedelk.database.internal.commons;

import com.reedelk.runtime.api.message.content.DataRow;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class JDBCDataRow implements DataRow<Serializable> {

    private final JDBCRowMetadata metadata;
    private final List<Serializable> row;

    JDBCDataRow(JDBCRowMetadata metadata, List<Serializable> row) {
        this.metadata = metadata;
        this.row = Collections.unmodifiableList(row);
    }

    @Override
    public int columnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public String columnName(int i) {
        return metadata.getColumnName(i);
    }

    @Override
    public List<String> columnNames() {
        return metadata.getColumnNames();
    }

    @Override
    public Serializable get(int column) {
        return row.get(column - 1);
    }

    @Override
    public Serializable getByColumnName(String columnName) {
        int index = metadata.getColumnIndex(columnName);
        return row.get(index - 1);
    }

    @Override
    public List<Serializable> values() {
        return row;
    }

    @Override
    public String toString() {
        return "JDBCDataRow{" +
                "columnNames=" + metadata.getColumnNames() +
                ", values=" + row +
                '}';
    }
}
