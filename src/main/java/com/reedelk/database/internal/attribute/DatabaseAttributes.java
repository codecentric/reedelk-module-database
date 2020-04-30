package com.reedelk.database.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import static com.reedelk.database.internal.attribute.DatabaseAttributes.QUERY;

@Type
@TypeProperty(name = QUERY, type = String.class)
public class DatabaseAttributes extends MessageAttributes {

    static final String QUERY = "query";

    public DatabaseAttributes(String query) {
        put(QUERY, query);
    }
}
