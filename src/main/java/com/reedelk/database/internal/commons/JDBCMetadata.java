package com.reedelk.database.internal.commons;

import com.reedelk.runtime.api.commons.SerializableUtils;
import com.reedelk.runtime.api.exception.PlatformException;

import java.io.Serializable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class JDBCMetadata extends HashMap<String, Serializable> {

    private static final String ATTRIBUTE_COLUMN_COUNT = "columnCount";
    private static final String ATTRIBUTE_COLUMN_NAMES = "columnNames";
    private static final String ATTRIBUTE_COLUMN_TYPES = "columnTypes";
    private static final String ATTRIBUTE_COLUMN_NAME_INDEX_MAP = "columnNameIndexMap";

    private JDBCMetadata(int columnCount,
                         List<String> columnNames,
                         List<Integer> columnTypes,
                         Map<String, Integer> columnNameIndexMap) {
        put(ATTRIBUTE_COLUMN_COUNT, columnCount);
        put(ATTRIBUTE_COLUMN_NAMES, SerializableUtils.asSerializableList(columnNames));
        put(ATTRIBUTE_COLUMN_TYPES, SerializableUtils.asSerializableList(columnTypes));
        put(ATTRIBUTE_COLUMN_NAME_INDEX_MAP, SerializableUtils.asSerializableMap(columnNameIndexMap));
    }

    public int getColumnCount() {
        return (int) get(ATTRIBUTE_COLUMN_COUNT);
    }

    public List<String> getColumnNames() {
        return (List<String>) get(ATTRIBUTE_COLUMN_NAMES);
    }

    public String getColumnName(int column) {
        return getColumnNames().get(column - 1);
    }

    public int getColumnIndex(String columnName) {
        return ((Map<String,Integer>) get(ATTRIBUTE_COLUMN_NAME_INDEX_MAP)).get(columnName);
    }

    public int getColumnType(int columnId) {
        return ((List<Integer>)get(ATTRIBUTE_COLUMN_TYPES)).get(columnId - 1);
    }

    public static JDBCMetadata from(ResultSetMetaData metadata) {
        try {
            int columnCount = metadata.getColumnCount();
            List<String> columnNames = getColumnNames(metadata);
            List<Integer> columnTypes = getColumnType(metadata);
            HashMap<String, Integer> columnNameIndexMap = getColumnIndex(metadata);
            return new JDBCMetadata(columnCount, columnNames, columnTypes, columnNameIndexMap);
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
            return columnNames;
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }

    private static List<Integer> getColumnType(ResultSetMetaData metadata) {
        ArrayList<Integer> columnTypes = new ArrayList<>();
        try {
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnTypes.add(metadata.getColumnType(i));
            }
            return columnTypes;
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }

    private static HashMap<String, Integer> getColumnIndex(ResultSetMetaData metadata) {
        HashMap<String, Integer> columnNameIndexMap = new HashMap<>();
        try {
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                columnNameIndexMap.put(metadata.getColumnName(i), i);
            }
            return columnNameIndexMap;
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }
}
