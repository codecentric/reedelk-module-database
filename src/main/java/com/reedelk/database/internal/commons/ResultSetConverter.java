package com.reedelk.database.internal.commons;

import com.reedelk.database.internal.exception.ConversionError;
import com.reedelk.runtime.api.commons.ByteArrayUtils;
import com.reedelk.runtime.api.message.content.DataRow;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.reedelk.database.internal.commons.Messages.Select.BLOB_TO_BYTES_ERROR;
import static com.reedelk.database.internal.commons.Messages.Select.COLUMN_TYPE_NOT_SUPPORTED;

public class ResultSetConverter {

    public static DataRow<Serializable> convertRow(JDBCMetadata metaData, ResultSet resultSetRow) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<Serializable> row = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            row.add(getObjectByColumnId(metaData, i, resultSetRow));
        }
        return new JDBCDataRow(metaData, row);
    }

    private static Serializable getObjectByColumnId(JDBCMetadata metaData, int columnId, ResultSet resultSetRow) throws SQLException {
        int columnType = metaData.getColumnType(columnId);
        int sqlColumnIndex = columnId + 1; // starts from 1 instead of zero ...
        if (columnType == java.sql.Types.BIGINT) {
            return resultSetRow.getInt(sqlColumnIndex);
        } else if (columnType == java.sql.Types.BOOLEAN) {
            return resultSetRow.getBoolean(sqlColumnIndex);
        } else if (columnType == java.sql.Types.DOUBLE) {
            return resultSetRow.getDouble(sqlColumnIndex);
        } else if (columnType == java.sql.Types.FLOAT) {
            return resultSetRow.getFloat(sqlColumnIndex);
        } else if (columnType == java.sql.Types.INTEGER) {
            return resultSetRow.getInt(sqlColumnIndex);
        } else if (columnType == java.sql.Types.NVARCHAR) {
            return resultSetRow.getNString(sqlColumnIndex);
        } else if (columnType == java.sql.Types.VARCHAR) {
            return resultSetRow.getString(sqlColumnIndex);
        } else if (columnType == java.sql.Types.TINYINT) {
            return resultSetRow.getInt(sqlColumnIndex);
        } else if (columnType == java.sql.Types.SMALLINT) {
            return resultSetRow.getInt(sqlColumnIndex);
        } else if (columnType == java.sql.Types.DATE) {
            return resultSetRow.getDate(sqlColumnIndex);
        } else if (columnType == java.sql.Types.TIMESTAMP) {
            return resultSetRow.getTimestamp(sqlColumnIndex);
        } else if (columnType == java.sql.Types.BLOB) {
            Blob blob = resultSetRow.getBlob(sqlColumnIndex);
            try (InputStream inputStream = blob.getBinaryStream()){
                return ByteArrayUtils.from(inputStream);
            } catch (IOException exception) {
                String columnName = metaData.getColumnName(sqlColumnIndex);
                String error = BLOB_TO_BYTES_ERROR.format(columnName);
                throw new ConversionError(error);
            }
        } else {
            String columnName = metaData.getColumnName(columnId);
            String error = COLUMN_TYPE_NOT_SUPPORTED.format(columnType, columnName);
            throw new ConversionError(error);
        }
    }
}
