package com.reedelk.mysql;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetConverter {

    public static JSONArray convert(ResultSet rs) throws SQLException, JSONException {
        JSONArray json = new JSONArray();
        ResultSetMetaData metaData = rs.getMetaData();

        while(rs.next()) {
            int numColumns = metaData.getColumnCount();
            JSONObject obj = new JSONObject();

            for (int i = 1; i < numColumns + 1; i++) {

                String column_name = metaData.getColumnName(i);

                if(metaData.getColumnType(i)==java.sql.Types.ARRAY){
                    obj.put(column_name, rs.getArray(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.BIGINT){
                    obj.put(column_name, rs.getInt(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.BOOLEAN){
                    obj.put(column_name, rs.getBoolean(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.BLOB){
                    obj.put(column_name, rs.getBlob(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.DOUBLE){
                    obj.put(column_name, rs.getDouble(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.FLOAT){
                    obj.put(column_name, rs.getFloat(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.INTEGER){
                    obj.put(column_name, rs.getInt(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.NVARCHAR){
                    obj.put(column_name, rs.getNString(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.VARCHAR){
                    obj.put(column_name, rs.getString(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.TINYINT){
                    obj.put(column_name, rs.getInt(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.SMALLINT){
                    obj.put(column_name, rs.getInt(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.DATE){
                    obj.put(column_name, rs.getDate(column_name));
                }
                else if(metaData.getColumnType(i)==java.sql.Types.TIMESTAMP){
                    obj.put(column_name, rs.getTimestamp(column_name));
                }
                else{
                    obj.put(column_name, rs.getObject(column_name));
                }
            }

            json.put(obj);
        }

        return json;
    }
}
