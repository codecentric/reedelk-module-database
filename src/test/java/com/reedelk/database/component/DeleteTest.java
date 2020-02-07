package com.reedelk.database.component;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.zapodot.junit.db.annotations.EmbeddedDatabaseTest;
import org.zapodot.junit.db.common.Engine;

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512));"
                + "INSERT INTO Customer(id, name) VALUES (1, 'John Doe');"
                + "INSERT INTO Customer(id, name) VALUES (2, 'Francis Lane');"
)
@ExtendWith(MockitoExtension.class)
class DeleteTest {

    @Test
    void something() {

    }
}
