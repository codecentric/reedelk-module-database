package de.codecentric.reedelk.database.internal.ddlexecute;

import de.codecentric.reedelk.runtime.api.annotation.DisplayName;

public enum DDLDefinitionStrategy {

    @DisplayName("From inline DDL")
    INLINE,
    @DisplayName("From DDL file")
    FROM_FILE
}
