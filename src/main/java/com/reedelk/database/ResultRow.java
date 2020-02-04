package com.reedelk.database;

import com.reedelk.runtime.api.exception.ESBException;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class ResultRow implements Serializable {

    private transient final ResultSetMetaData metadata;
    private final List<Object> row;

    public ResultRow(ResultSetMetaData metadata, List<Object> row) {
        this.metadata = metadata;
        this.row = row;
    }

    public int getColumnCount() {
        try {
            return metadata.getColumnCount();
        } catch (SQLException e) {
            throw new ESBException(e);
        }
    }

    public String getColumnName(int i) {
        try {
            return metadata.getColumnName(i);
        } catch (SQLException e) {
            throw new ESBException(e);
        }
    }

    public Object get(int column) {
        return row.get(column - 1);
    }

    @Override
    public String toString() {
        return "ResultRow{" +
                "row=" + row +
                '}';
    }
}
