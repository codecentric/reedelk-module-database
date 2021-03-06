package de.codecentric.reedelk.database.component;

import de.codecentric.reedelk.database.internal.commons.DataSourceService;
import de.codecentric.reedelk.database.internal.commons.DatabaseDriver;
import de.codecentric.reedelk.runtime.api.commons.ModuleContext;
import de.codecentric.reedelk.runtime.api.component.Component;
import de.codecentric.reedelk.runtime.api.exception.PlatformException;
import de.codecentric.reedelk.runtime.api.flow.FlowContext;
import de.codecentric.reedelk.runtime.api.message.Message;
import de.codecentric.reedelk.runtime.api.message.MessageBuilder;
import de.codecentric.reedelk.runtime.api.script.ScriptEngineService;
import de.codecentric.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

import static de.codecentric.reedelk.runtime.api.commons.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512));"
                + "INSERT INTO Customer(id, name) VALUES (1, 'John Doe');"
                + "INSERT INTO Customer(id, name) VALUES (2, 'Francis Lane');"
)
@ExtendWith(MockitoExtension.class)
class DeleteTest {

    @Mock
    private ScriptEngineService mockScriptEngine;
    @Mock
    private FlowContext mockFlowContext;

    private ModuleContext moduleContext = new ModuleContext(1L);

    private Delete component = new Delete();

    private Message testMessage;

    @BeforeEach
    void setUp() {
        testMessage = MessageBuilder.get((Class<? extends Component>) TestComponent.class).withText("Test").build();
        lenient()
                .doReturn(new HashMap<>())
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL("jdbc:h2:mem:" + DeleteTest.class.getSimpleName());
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
    void shouldDeleteRowCorrectly(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        component.setQuery("DELETE FROM Customer WHERE id = 1;");
        component.initialize();

        ResultSet resultSet = dataSource
                .getConnection()
                .createStatement()
                .executeQuery("SELECT * FROM Customer WHERE id = 1");

        assertThat(resultSet.next()).isTrue();


        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        int deleted = actual.payload();
        assertThat(deleted).isEqualTo(1);

        resultSet = dataSource
                .getConnection()
                .createStatement()
                .executeQuery("SELECT * FROM Customer WHERE id = 1");
        assertThat(resultSet.next()).isFalse();
    }

    @Test
    void shouldDeleteRowCorrectlyWhenParameterizedQuery(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        DynamicObjectMap map =
                DynamicObjectMap.from(of("id", "#[2]"), moduleContext);

        lenient()
                .doReturn(of("id", 2))
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        component.setQuery("DELETE FROM Customer WHERE id=:id");
        component.setParametersMapping(map);
        component.initialize();

        ResultSet resultSet = dataSource
                .getConnection()
                .createStatement()
                .executeQuery("SELECT * FROM Customer WHERE id = 2");
        assertThat(resultSet.next()).isTrue();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        int inserted = actual.payload();
        assertThat(inserted).isEqualTo(1);

        resultSet = dataSource
                .getConnection()
                .createStatement()
                .executeQuery("SELECT * FROM Customer WHERE id = 2");
        assertThat(resultSet.next()).isFalse();
    }

    @Disabled
    void shouldIncludeStatementWhenExceptionThrown() {
        // Given
        component.setQuery("DELETE FROM Customer = 1;");
        component.initialize();

        // When
        PlatformException thrown = assertThrows(PlatformException.class,
                () -> component.apply(mockFlowContext, testMessage));

        assertThat(thrown).isNotNull();
        assertThat(thrown).hasMessage("Could not execute delete query=[DELETE FROM Customer = 1;]: Syntax error in SQL statement \"DELETE FROM CUSTOMER =[*] 1;\"; SQL statement:\n" +
                "DELETE FROM Customer = 1; [42000-200]");
    }
}
