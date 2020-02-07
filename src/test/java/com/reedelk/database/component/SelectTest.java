package com.reedelk.database.component;

import com.reedelk.database.commons.DataSourceService;
import com.reedelk.database.commons.DatabaseDriver;
import com.reedelk.database.commons.ResultRow;
import com.reedelk.database.configuration.ConnectionConfiguration;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
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
import java.sql.SQLException;
import java.util.*;

import static com.reedelk.runtime.api.commons.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThat;
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

    @BeforeEach
    void setUp() {
        lenient()
                .doReturn(new HashMap<>())
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL("jdbc:h2:mem:SelectTest");
        connectionConfiguration.setDatabaseDriver(DatabaseDriver.H2);
        component.setConnectionConfiguration(connectionConfiguration);
        component.dataSourceService = new DataSourceService();
        component.scriptEngine = mockScriptEngine;
    }

    @AfterEach
    void tearDown(@EmbeddedDatabase final DataSource dataSource) {
        try {
            dataSource.getConnection().createStatement().execute("DROP TABLE Customer");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    void shouldReturnAllRowsFromTable() {
        // Given
        Message message = MessageBuilder.get().withText("Test").build();
        component.setQuery("SELECT * FROM Customer");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, message);

        // Then
        List<ResultRow> result = actual.payload();
        assertFound(result, of("id", 1, "name", "John Doe"));
        assertFound(result, of("id", 2, "name", "Mark Anton"));
    }

    @Test
    void shouldReturnEmptyResultSet() {
        // Given
        Message message = MessageBuilder.get().withText("Test").build();
        component.setQuery("SELECT * FROM customer WHERE id = 4;");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, message);

        // Then
        List<ResultRow> result = actual.payload();
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnRowsMatchingWhereClause() {
        // Given
        Message message = MessageBuilder.get().withText("Test").build();
        component.setQuery("SELECT * FROM customer WHERE id = 2");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, message);

        // Then
        List<ResultRow> result = actual.payload();
        assertThat(result).hasSize(1);
        assertFound(result, of("id", 2, "name", "Mark Anton"));
    }

    @Test
    void shouldReturnRowsMatchingParameterizedQueryWithLike() {
        // Given
        Message message = MessageBuilder.get().withText("Test").build();
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
        Message actual = component.apply(mockFlowContext, message);

        // Then
        List<ResultRow> result = actual.payload();
        assertThat(result).hasSize(1);
        assertFound(result, of("id", 2, "name", "Mark Anton"));
    }

    @Test
    void shouldReturnRowsMatchingParameterizedQueryWithExactMatch() {
        // Given
        Message message = MessageBuilder.get().withText("Test").build();
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
        Message actual = component.apply(mockFlowContext, message);

        // Then
        List<ResultRow> result = actual.payload();
        assertThat(result).hasSize(1);
        assertFound(result, of("id", 1, "name", "John Doe"));
    }

    private void assertFound(Collection<ResultRow> rows, Map<String, Object> columnNameAndValueMap) {
        boolean found = findRowInCollection(rows, columnNameAndValueMap);
        assertThat(found).isTrue();
    }

    private boolean findRowInCollection(Collection<ResultRow> rows, Map<String, Object> columnNameAndValueMap) {
        for (ResultRow current : rows) {
            if (sameRow(current, columnNameAndValueMap)) {
                return true;
            }
        }
        return false;
    }

    private boolean sameRow(ResultRow given, Map<String, Object> columnNameAndValueMap) {
        boolean[] matches = new boolean[]{true};
        columnNameAndValueMap.forEach((columnName, columnValue) -> {
            int columnCount = given.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String currentColName = given.getColumnName(i);
                if (currentColName.equals(columnName)) {
                    matches[0] = Objects.equals(given.get(i), columnValue);
                }
            }
        });
        return matches[0];
    }
}
