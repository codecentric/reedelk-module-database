package de.codecentric.reedelk.database.internal.attribute;

import de.codecentric.reedelk.runtime.api.annotation.Type;
import de.codecentric.reedelk.runtime.api.annotation.TypeProperty;
import de.codecentric.reedelk.runtime.api.message.MessageAttributes;

import static de.codecentric.reedelk.database.internal.attribute.DatabaseAttributes.QUERY;

@Type
@TypeProperty(name = QUERY, type = String.class)
public class DatabaseAttributes extends MessageAttributes {

    static final String QUERY = "query";

    public DatabaseAttributes(String query) {
        put(QUERY, query);
    }
}
