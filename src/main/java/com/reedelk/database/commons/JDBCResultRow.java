package com.reedelk.database.commons;

import com.reedelk.runtime.api.message.content.ResultRow;

import java.util.Collections;
import java.util.List;

public class JDBCResultRow implements ResultRow {

    private final JDBCRowMetadata metadata;
    private final List<Object> row;

    JDBCResultRow(JDBCRowMetadata metadata, List<Object> row) {
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
    public Object get(int column) {
        return row.get(column - 1);
    }

    @Override
    public Object getByColumnName(String columnName) {
        int index = metadata.getColumnIndex(columnName);
        return row.get(index);
    }

    @Override
    public List<Object> row() {
        return row;
    }

    @Override
    public String toString() {
        return "JDBCResultRow{" +
                "metadata=" + metadata +
                ", row=" + row +
                '}';
    }
}
