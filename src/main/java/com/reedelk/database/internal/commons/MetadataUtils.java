package com.reedelk.database.internal.commons;

import com.reedelk.runtime.api.exception.PlatformException;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataUtils {

    private MetadataUtils() {
    }

    public static List<Integer> getColumnType(ResultSetMetaData metadata) {
        List<Integer> columnTypes = new ArrayList<>();
        try {
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                columnTypes.add(metadata.getColumnType(i + 1)); // Index Starts from 1 instead of 0
            }
            return columnTypes;
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }

    public static Map<String, Integer> getColumnNameIndexMap(ResultSetMetaData metadata) {
        Map<String, Integer> columnNameIndexMap = new HashMap<>();
        try {
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                columnNameIndexMap.put(metadata.getColumnName(i + 1), i); // Index Starts from 1 instead of 0
            }
            return columnNameIndexMap;
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }

    public static Map<Integer, String> getColumnIndexNameMap(ResultSetMetaData metadata) {
        Map<Integer, String> columnIndexNameMap = new HashMap<>();
        try {
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                columnIndexNameMap.put(i, metadata.getColumnName(i + 1)); // Index Starts from 1 instead of 0
            }
            return columnIndexNameMap;
        } catch (SQLException exception) {
            throw new PlatformException(exception);
        }
    }
}
