package com.reedelk.database.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.commons.SerializableUtils;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.List;

import static com.reedelk.database.internal.attribute.SelectAttributes.COLUMN_TYPES;
import static com.reedelk.database.internal.attribute.SelectAttributes.QUERY;

@Type
@TypeProperty(name = QUERY, type = String.class)
@TypeProperty(name = COLUMN_TYPES, type = String.class)
public class SelectAttributes extends MessageAttributes {

    static final String QUERY = "query";
    static final String COLUMN_TYPES = "columnTypes";

    public SelectAttributes(String query, List<Integer> columnTypes) {
        put(QUERY, query);
        put(COLUMN_TYPES, SerializableUtils.asSerializableList(columnTypes));
    }
}
