package com.reedelk.database.component;

import com.reedelk.database.internal.commons.DataSourceService;
import com.reedelk.database.internal.commons.DatabaseDriver;
import com.reedelk.database.internal.type.DatabaseRow;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import com.reedelk.runtime.api.type.MapOfStringSerializable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.zapodot.junit.db.annotations.EmbeddedDatabase;
import org.zapodot.junit.db.annotations.EmbeddedDatabaseTest;
import org.zapodot.junit.db.common.Engine;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

import static com.reedelk.runtime.api.commons.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512)); "
                + "INSERT INTO CUSTOMER(id, name) VALUES (1, 'John Doe');"
                + "INSERT INTO CUSTOMER(id, name) VALUES (2, 'Mark Anton');"
)
@ExtendWith(MockitoExtension.class)
class SelectTest {

    @Mock
    private ScriptEngineService mockScriptEngine;
    @Mock
    private FlowContext mockFlowContext;

    private ModuleContext moduleContext = new ModuleContext(1L);

    private Select component = new Select();

    private Message testMessage;

    @BeforeEach
    void setUp() {
        testMessage = MessageBuilder.get(TestComponent.class).withText("Test").build();
        lenient()
                .doReturn(new HashMap<>())
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL("jdbc:h2:mem:" + SelectTest.class.getSimpleName());
        connectionConfiguration.setDatabaseDriver(DatabaseDriver.H2);
        component.setConnection(connectionConfiguration);
        component.dataSourceService = new DataSourceService();
        component.scriptEngine = mockScriptEngine;
    }

    @AfterEach
    void tearDown(@EmbeddedDatabase final DataSource dataSource) {
        try {
            dataSource.getConnection().createStatement().execute("DROP TABLE Customer");
        } catch (SQLException exception) {
            // Nothing we can really do here.
            exception.printStackTrace();
        }
    }

    @Test
    void shouldReturnAllRowsFromTable() {
        // Given
        component.setQuery("SELECT * FROM Customer");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        List<DatabaseRow> result = actual.payload();
        assertFound(result, of("ID", 1, "NAME", "John Doe"));
        assertFound(result, of("ID", 2, "NAME", "Mark Anton"));
    }

    @Test
    void shouldReturnEmptyResultSet() {
        // Given
        component.setQuery("SELECT * FROM customer WHERE id = 4;");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        List<MapOfStringSerializable> result = actual.payload();
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnRowsMatchingWhereClause() {
        // Given
        component.setQuery("SELECT * FROM customer WHERE id = 2");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        List<DatabaseRow> result = actual.payload();
        assertThat(result).hasSize(1);
        assertFound(result, of("ID", 2, "NAME", "Mark Anton"));
    }

    @Test
    void shouldReturnRowsMatchingParameterizedQueryWithLike() {
        // Given
        DynamicObjectMap map =
                DynamicObjectMap.from(of("paramName", "#['Mar']"), moduleContext);

        lenient()
                .doReturn(of("paramName", "Mark%"))
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        component.setQuery("SELECT * FROM customer WHERE name LIKE :paramName");
        component.setParametersMapping(map);
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        List<DatabaseRow> result = actual.payload();
        assertThat(result).hasSize(1);
        assertFound(result, of("ID", 2, "NAME", "Mark Anton"));
    }

    @Test
    void shouldReturnRowsMatchingParameterizedQueryWithExactMatch() {
        // Given
        DynamicObjectMap map =
                DynamicObjectMap.from(of("id", "#[1]"), moduleContext);

        lenient()
                .doReturn(of("id", 1))
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        component.setQuery("SELECT * FROM customer WHERE id = :id;");
        component.setParametersMapping(map);
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        List<DatabaseRow> result = actual.payload();
        assertThat(result).hasSize(1);
        assertFound(result, of("ID", 1, "NAME", "John Doe"));
    }

    @Test
    void shouldIncludeStatementWhenExceptionThrown() {
        // Given
        component.setQuery("SELECT WHERE customer WHERE id = 2");
        component.initialize();

        // When
        PlatformException thrown = assertThrows(PlatformException.class,
                () -> component.apply(mockFlowContext, testMessage));

        assertThat(thrown).isNotNull();
        assertThat(thrown).hasMessage("Could not execute select query=[SELECT WHERE customer WHERE id = 2]: Syntax error in SQL statement \"SELECT WHERE CUSTOMER WHERE[*] ID = 2\"; SQL statement:\n" +
                "SELECT WHERE customer WHERE id = 2 [42000-200]");
    }

    private void assertFound(Collection<DatabaseRow> rows, Map<String, Serializable> columnNameAndValueMap) {
        boolean found = findRowInCollection(rows, columnNameAndValueMap);
        assertThat(found).isTrue();
    }

    private boolean findRowInCollection(Collection<DatabaseRow> rows, Map<String, Serializable> columnNameAndValueMap) {
        for (DatabaseRow current : rows) {
            boolean sameRow = equals(current, columnNameAndValueMap);
            if (sameRow) return true;
        }
        return false;
    }

    private boolean equals(DatabaseRow current, Map<String, Serializable> columnNameAndValueMap) {
        for (Map.Entry<String, Serializable> entry : columnNameAndValueMap.entrySet()) {
            String expectedKey = entry.getKey();
            Serializable expectedValue = entry.getValue();
            if (!current.containsKey(expectedKey)) return false;
            Serializable actualValue = current.get(expectedKey);
            if (!Objects.equals(actualValue, expectedValue)) return false;
        }
        return true;
    }
}
