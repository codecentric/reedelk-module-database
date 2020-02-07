package com.reedelk.database.component;

import com.reedelk.database.commons.DataSourceService;
import com.reedelk.database.commons.DatabaseDriver;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import static com.reedelk.runtime.api.commons.ImmutableMap.of;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512));"
                + "INSERT INTO Customer(id, name) VALUES (1, 'John Doe');"
)
@ExtendWith(MockitoExtension.class)
class InsertTest {

    @Mock
    private ScriptEngineService mockScriptEngine;
    @Mock
    private FlowContext mockFlowContext;

    private ModuleContext moduleContext = new ModuleContext(1L);

    private Insert component = new Insert();

    private Message testMessage;

    @BeforeEach
    void setUp() {
        testMessage = MessageBuilder.get().withText("Test").build();
        lenient()
                .doReturn(new HashMap<>())
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL("jdbc:h2:mem:" + InsertTest.class.getSimpleName());
        connectionConfiguration.setDatabaseDriver(DatabaseDriver.H2);
        component.setConnectionConfiguration(connectionConfiguration);
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
    void shouldInsertRowCorrectly(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        component.setQuery("INSERT INTO Customer VALUES (2,'Mark Anton')");
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        int inserted = actual.payload();
        assertThat(inserted).isEqualTo(1);

        ResultSet resultSet = dataSource.getConnection().createStatement().executeQuery("SELECT * FROM Customer WHERE id = 2");
        resultSet.next();
        int actualId = resultSet.getInt(1);
        assertThat(actualId).isEqualTo(2);
        String actualName = resultSet.getString(2);
        assertThat(actualName).isEqualTo("Mark Anton");
    }

    @Test
    void shouldInsertRowCorrectlyWhenParameterizedQuery(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        DynamicObjectMap map =
                DynamicObjectMap.from(of("id", "#[4]", "name", "#['Michael S. Madden']"), moduleContext);

        lenient()
                .doReturn(of("id", 4, "name", "Michael S. Madden"))
                .when(mockScriptEngine)
                .evaluate(any(DynamicObjectMap.class), any(FlowContext.class), any(Message.class));

        component.setQuery("INSERT INTO Customer VALUES (:id,:name)");
        component.setParametersMapping(map);
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then
        int inserted = actual.payload();
        assertThat(inserted).isEqualTo(1);

        ResultSet resultSet = dataSource.getConnection().createStatement().executeQuery("SELECT * FROM Customer WHERE id = 4");
        resultSet.next();
        int actualId = resultSet.getInt(1);
        assertThat(actualId).isEqualTo(4);
        String actualName = resultSet.getString(2);
        assertThat(actualName).isEqualTo("Michael S. Madden");
    }
}
