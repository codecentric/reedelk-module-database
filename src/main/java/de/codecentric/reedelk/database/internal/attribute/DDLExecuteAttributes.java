package de.codecentric.reedelk.database.internal.attribute;

import de.codecentric.reedelk.runtime.api.annotation.Type;
import de.codecentric.reedelk.runtime.api.annotation.TypeProperty;
import de.codecentric.reedelk.runtime.api.message.MessageAttributes;

@Type
@TypeProperty(name = DDLExecuteAttributes.DDL, type = String.class)
public class DDLExecuteAttributes extends MessageAttributes {

    static final String DDL = "ddl";

    public DDLExecuteAttributes(String ddl) {
        put(DDL, ddl);
    }
}
