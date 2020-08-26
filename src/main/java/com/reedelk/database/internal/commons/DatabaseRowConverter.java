package com.reedelk.database.internal.commons;

import com.reedelk.database.internal.exception.ConversionError;
import com.reedelk.database.internal.type.DatabaseRow;
import com.reedelk.runtime.api.commons.ByteArrayUtils;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.reedelk.database.internal.commons.Messages.Select.*;

public class DatabaseRowConverter {

    public static DatabaseRow convert(ResultSetMetaData metaData,
                                      ResultSet resultSetRow,
                                      Map<String, Integer> columnNameIndexMap,
                                      Map<Integer, String> columnIndexNameMap) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<Serializable> values = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            Serializable rowValue = getObjectByColumnId(metaData, i, resultSetRow);
            values.add(rowValue);
        }
        return new DatabaseRow(columnNameIndexMap, columnIndexNameMap, values);
    }

    private static Serializable getObjectByColumnId(ResultSetMetaData metaData, int columnId, ResultSet row) throws SQLException {
        int columnType = metaData.getColumnType(columnId);
        if (columnType == Types.CHAR) return row.getString(columnId);
        if (columnType == Types.VARCHAR) return row.getString(columnId);
        if (columnType == Types.LONGNVARCHAR) return row.getString(columnId);
        if (columnType == Types.NUMERIC) return row.getBigDecimal(columnId);
        if (columnType == Types.DECIMAL) return row.getBigDecimal(columnId);
        if (columnType == Types.BIT) return row.getBoolean(columnId);
        if (columnType == Types.TINYINT) return row.getByte(columnId);
        if (columnType == Types.SMALLINT) return row.getShort(columnId);
        if (columnType == Types.INTEGER) return row.getInt(columnId);
        if (columnType == Types.BIGINT) return row.getLong(columnId);
        if (columnType == Types.REAL) return row.getFloat(columnId);
        if (columnType == Types.FLOAT) return row.getDouble(columnId);
        if (columnType == Types.DOUBLE) return row.getDouble(columnId);
        if (columnType == Types.BINARY) return row.getBytes(columnId);
        if (columnType == Types.VARBINARY) return row.getBytes(columnId);
        if (columnType == Types.LONGVARBINARY) return row.getBytes(columnId);
        if (columnType == Types.DATE) return row.getDate(columnId);
        if (columnType == Types.TIME) return row.getTime(columnId);
        if (columnType == Types.TIMESTAMP) return row.getTimestamp(columnId);
        if (columnType == Types.BOOLEAN) return row.getBoolean(columnId);
        if (columnType == Types.NVARCHAR) return row.getNString(columnId);
        if (columnType == Types.BLOB) {
            Blob blob = row.getBlob(columnId);
            try (InputStream inputStream = blob.getBinaryStream()) {
                return ByteArrayUtils.from(inputStream);
            } catch (IOException exception) {
                String columnName = metaData.getColumnName(columnId);
                String error = BLOB_TO_BYTES_ERROR.format(columnName);
                throw new ConversionError(error, exception);
            }
        }
        if (columnType == Types.CLOB) {
            Clob clob = row.getClob(columnId);
            return clobToString(metaData, columnId, clob);
        }

        String columnName = metaData.getColumnName(columnId);
        String error = COLUMN_TYPE_NOT_SUPPORTED.format(columnType, columnName);
        throw new ConversionError(error);
    }

    private static String clobToString(ResultSetMetaData metaData, int columnId, java.sql.Clob data) throws SQLException {
        final StringBuilder sb = new StringBuilder();
        try (Reader reader = data.getCharacterStream();
             BufferedReader br = new BufferedReader(reader)) {
            int b;
            while(-1 != (b = br.read())) {
                sb.append((char)b);
            }
            return sb.toString();
        } catch (SQLException | IOException exception) {
            String columnName = metaData.getColumnName(columnId);
            String error = CLOB_TO_STRING_ERROR.format(columnName);
            throw new ConversionError(error);
        }
    }
}
