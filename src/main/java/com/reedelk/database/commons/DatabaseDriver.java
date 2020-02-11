package com.reedelk.database.commons;

import com.reedelk.runtime.api.annotation.DisplayName;

public enum DatabaseDriver {

    @DisplayName("MySQL (com.mysql.cj.jdbc.Driver)")
    MYSQL("com.mysql.cj.jdbc.Driver"),
    @DisplayName("Oracle (oracle.jdbc.OracleDriver)")
    ORACLE("oracle.jdbc.OracleDriver"),
    @DisplayName("PostgreSQL (org.postgresql.Driver)")
    POSTGRESQL("org.postgresql.Driver"),
    @DisplayName("H2 (org.h2.Driver)")
    H2("org.h2.Driver");

    private final String qualifiedName;

    DatabaseDriver(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

}
