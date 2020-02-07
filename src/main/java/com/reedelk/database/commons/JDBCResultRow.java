package com.reedelk.database.commons;

import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.message.content.ResultRow;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JDBCResultRow implements ResultRow {

    private transient final ResultSetMetaData metadata;
    private final List<Object> row;

    JDBCResultRow(ResultSetMetaData metadata, List<Object> row) {
        this.metadata = metadata;
        this.row = row;
    }

    @Override
    public int getColumnCount() {
        try {
            return metadata.getColumnCount();
        } catch (SQLException e) {
            throw new ESBException(e);
        }
    }

    @Override
    public String getColumnName(int i) {
        try {
            return metadata.getColumnName(i);
        } catch (SQLException e) {
            throw new ESBException(e);
        }
    }

    @Override
    public List<String> columnNames() {
        List<String> columnNames = new ArrayList<>();
        try {
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnNames.add(metadata.getColumnName(i));
            }
            return Collections.unmodifiableList(columnNames);
        } catch (SQLException e) {
            throw new ESBException(e);
        }
    }

    @Override
    public Object get(int column) {
        return row.get(column - 1);
    }

    @Override
    public List<Object> row() {
        return Collections.unmodifiableList(row);
    }

    @Override
    public String toString() {
        return "ResultRow{" +
                "row=" + row +
                '}';
    }
}
