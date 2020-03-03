package com.reedelk.database.commons;

import com.reedelk.runtime.api.message.content.ResultRow;
import com.reedelk.runtime.api.message.content.TypedPublisher;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import reactor.core.publisher.Flux;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetConverter {

    public static JSONArray convert(TypedPublisher<ResultRow> resultSetFlux) throws JSONException {
        JSONArray json = new JSONArray();
        Flux.from(resultSetFlux).subscribe(resultSetRow -> {
            JSONObject rowObject = new JSONObject();
            int numColumns = resultSetRow.columnCount();
            for (int i = 1; i < numColumns + 1; i++) {
                String columnName = resultSetRow.columnName(i);
                rowObject.put(columnName, resultSetRow.get(i));
            }
            json.put(rowObject);
        });

        return json;
    }

    public static ResultRow convertRow(ResultSetMetaData metaData, ResultSet resultSetRow) throws SQLException {
        int columnCount = metaData.getColumnCount();
        List<Object> row = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            row.add(getObjectByColumnId(metaData, i, resultSetRow));
        }
        return new JDBCResultRow(metaData, row);
    }

    private static Object getObjectByColumnId(ResultSetMetaData metaData, int columnId, ResultSet resultSetRow) throws SQLException {
        if (metaData.getColumnType(columnId) == java.sql.Types.ARRAY) {
            return resultSetRow.getArray(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.BIGINT) {
            return resultSetRow.getInt(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.BOOLEAN) {
            return resultSetRow.getBoolean(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.BLOB) {
            return resultSetRow.getBlob(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.DOUBLE) {
            return resultSetRow.getDouble(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.FLOAT) {
            return resultSetRow.getFloat(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.INTEGER) {
            return resultSetRow.getInt(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.NVARCHAR) {
            return resultSetRow.getNString(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.VARCHAR) {
            return resultSetRow.getString(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.TINYINT) {
            return resultSetRow.getInt(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.SMALLINT) {
            return resultSetRow.getInt(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.DATE) {
            return resultSetRow.getDate(columnId);
        } else if (metaData.getColumnType(columnId) == java.sql.Types.TIMESTAMP) {
            return resultSetRow.getTimestamp(columnId);
        } else {
            return resultSetRow.getObject(columnId);
        }
    }
}
