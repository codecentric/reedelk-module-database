package com.reedelk.database.component;

import com.reedelk.database.internal.commons.DataSourceService;
import com.reedelk.database.internal.commons.DatabaseDriver;
import com.reedelk.runtime.api.commons.ModuleContext;
import com.reedelk.runtime.api.exception.PlatformException;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static com.reedelk.runtime.api.commons.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512));"
                + "INSERT INTO Customer(id, name) VALUES (1, 'John Doe');"
)
@ExtendWith(MockitoExtension.class)
class UpdateTest {

    @Mock
    private ScriptEngineService mockScriptEngine;
    @Mock
    private FlowContext mockFlowContext;

    private ModuleContext moduleContext = new ModuleContext(1L);

    private Update component = new Update();

    private Message testMessage;

    @BeforeEach
    void setUp() {
        testMessage = MessageBuilder.get(TestComponent.class).withText("Test").build();
        lenient()
                .doReturn(new HashMap<>())
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL("jdbc:h2:mem:" + UpdateTest.class.getSimpleName());
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
    void shouldUpdateRowCorrectly(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        component.setQuery("UPDATE Customer SET name = 'Francis Lane' WHERE id = 1;");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        int updated = actual.payload();
        assertThat(updated).isEqualTo(1);

        ResultSet resultSet = dataSource.getConnection().createStatement().executeQuery("SELECT * FROM Customer WHERE id = 1");
        assertThat(resultSet.next()).isTrue();

        int actualId = resultSet.getInt(1);
        assertThat(actualId).isEqualTo(1);

        String actualName = resultSet.getString(2);
        assertThat(actualName).isEqualTo("Francis Lane");
    }

    @Test
    void shouldInsertRowCorrectlyWhenParameterizedQuery(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        DynamicObjectMap map =
                DynamicObjectMap.from(of("name", "#['Michael S. Madden']"), moduleContext);

        lenient()
                .doReturn(of("name", "Michael S. Madden"))
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        component.setQuery("UPDATE Customer SET name =:name WHERE id = 1;");
        component.setParametersMapping(map);
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        int updated = actual.payload();
        assertThat(updated).isEqualTo(1);

        ResultSet resultSet = dataSource
                .getConnection()
                .createStatement()
                .executeQuery("SELECT * FROM Customer WHERE id = 1");
        assertThat(resultSet.next()).isTrue();

        int actualId = resultSet.getInt(1);
        assertThat(actualId).isEqualTo(1);

        String actualName = resultSet.getString(2);
        assertThat(actualName).isEqualTo("Michael S. Madden");
    }

    @Test
    void shouldIncludeStatementWhenExceptionThrown() {
        // Given
        component.setQuery("UPDATE Customer SETWHERE id = 1;");
        component.initialize();

        // When
        PlatformException thrown = assertThrows(PlatformException.class,
                () -> component.apply(mockFlowContext, testMessage));

        assertThat(thrown).isNotNull();
        assertThat(thrown).hasMessage("Could not execute update query=[UPDATE Customer SETWHERE id = 1;]: Syntax error in SQL statement \"UPDATE CUSTOMER SETWHERE ID[*] = 1;\"; expected \"SET\"; SQL statement:\n" +
                "UPDATE Customer SETWHERE id = 1; [42001-200]");
    }
}
