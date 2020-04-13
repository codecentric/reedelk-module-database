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

    public static DataRow<Serializable> convertRow(JDBCRowMetadata metaData, ResultSet resultSetRow) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<Serializable> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            row.add(getObjectByColumnId(metaData, i, resultSetRow));
        }
        return new JDBCDataRow(metaData, row);
    }

    private static Serializable getObjectByColumnId(JDBCRowMetadata metaData, int columnId, ResultSet resultSetRow) throws SQLException {
        int columnType = metaData.getColumnType(columnId);
        if (columnType == java.sql.Types.BIGINT) {
            return resultSetRow.getInt(columnId);
        } else if (columnType == java.sql.Types.BOOLEAN) {
            return resultSetRow.getBoolean(columnId);
        } else if (columnType == java.sql.Types.DOUBLE) {
            return resultSetRow.getDouble(columnId);
        } else if (columnType == java.sql.Types.FLOAT) {
            return resultSetRow.getFloat(columnId);
        } else if (columnType == java.sql.Types.INTEGER) {
            return resultSetRow.getInt(columnId);
        } else if (columnType == java.sql.Types.NVARCHAR) {
            return resultSetRow.getNString(columnId);
        } else if (columnType == java.sql.Types.VARCHAR) {
            return resultSetRow.getString(columnId);
        } else if (columnType == java.sql.Types.TINYINT) {
            return resultSetRow.getInt(columnId);
        } else if (columnType == java.sql.Types.SMALLINT) {
            return resultSetRow.getInt(columnId);
        } else if (columnType == java.sql.Types.DATE) {
            return resultSetRow.getDate(columnId);
        } else if (columnType == java.sql.Types.TIMESTAMP) {
            return resultSetRow.getTimestamp(columnId);
        } else if (columnType == java.sql.Types.BLOB) {
            Blob blob = resultSetRow.getBlob(columnId);
            try (InputStream inputStream = blob.getBinaryStream()){
                return ByteArrayUtils.from(inputStream);
            } catch (IOException exception) {
                String columnName = metaData.getColumnName(columnId);
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
