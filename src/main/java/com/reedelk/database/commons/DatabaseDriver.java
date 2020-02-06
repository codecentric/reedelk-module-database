package com.reedelk.database.commons;

import com.reedelk.runtime.api.annotation.DisplayName;

public enum DatabaseDriver {

    @DisplayName("MySQL (com.mysql.cj.jdbc.Driver)")
    MYSQL("com.mysql.cj.jdbc.Driver"),
    @DisplayName("Oracle (oracle.jdbc.driver.OracleDriver)")
    ORACLE("oracle.jdbc.driver.OracleDriver"),
    @DisplayName("PostgreSQL (org.postgresql.Driver)")
    POSTGRESQL("org.postgresql.Driver");

    private final String qualifiedName;

    DatabaseDriver(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String qualifiedName() {
        return qualifiedName;
    }

}
