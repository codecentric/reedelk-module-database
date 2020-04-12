package com.reedelk.database.internal.commons;

import com.reedelk.runtime.api.exception.PlatformException;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class JDBCRowMetadata implements Serializable {

    private final int columnCount;
    private final List<String> columnNames;
    private final List<Integer> columnTypes;
    private final Map<String, Integer> columnNameIndexMap;

    public JDBCRowMetadata(int columnCount,
                           List<String> columnNames,
                           List<Integer> columnTypes,
                           Map<String, Integer> columnNameIndexMap) {
        this.columnCount = columnCount;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnNameIndexMap = columnNameIndexMap;
    }

    public int getColumnCount() {
        return columnCount;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public String getColumnName(int column) {
        return columnNames.get(column - 1);
    }

    public int getColumnIndex(String columnName) {
        return columnNameIndexMap.get(columnName);
    }

    public int getColumnType(int columnId) {
        return columnTypes.get(columnId - 1);
    }

    @Override
    public String toString() {
        return "JDBCRowMetadata{" +
                "columnCount=" + columnCount +
                ", columnNames=" + columnNames +
                ", columnTypes=" + columnTypes +
                ", columnNameIndexMap=" + columnNameIndexMap +
                '}';
    }

    public static JDBCRowMetadata from(ResultSetMetaData metadata) {
        try {
            int columnCount = metadata.getColumnCount();
            List<String> columnNames = getColumnNames(metadata);
            List<Integer> columnTypes = getColumnType(metadata);
            Map<String, Integer> columnNameIndexMap = getColumnIndex(metadata);
            return new JDBCRowMetadata(columnCount, columnNames, columnTypes, columnNameIndexMap);
        } catch (SQLException exception) {
            throw new PlatformException("Could not create result row metadata", exception);
        }
    }

    private static List<String> getColumnNames(ResultSetMetaData metadata) {
        List<String> columnNames = new ArrayList<>();
        try {
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnNames.add(metadata.getColumnName(i));
            }
            return Collections.unmodifiableList(columnNames);
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }

    private static List<Integer> getColumnType(ResultSetMetaData metadata) {
        List<Integer> columnTypes = new ArrayList<>();
        try {
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnTypes.add(metadata.getColumnType(i));
            }
            return Collections.unmodifiableList(columnTypes);
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }

    private static Map<String, Integer> getColumnIndex(ResultSetMetaData metadata) {
        Map<String, Integer> columnNameIndexMap = new HashMap<>();
        try {
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnNameIndexMap.put(metadata.getColumnName(i), i);
            }
            return Collections.unmodifiableMap(columnNameIndexMap);
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }
}
