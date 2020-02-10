package com.reedelk.database.configuration;

import com.reedelk.runtime.api.annotation.DisplayName;

public enum DDLDefinitionStrategy {

    @DisplayName("From inline DDL")
    INLINE,
    @DisplayName("From DDL file")
    FROM_FILE
}
