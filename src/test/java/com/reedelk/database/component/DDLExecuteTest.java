package com.reedelk.database.component;

import com.reedelk.database.internal.commons.DataSourceService;
import com.reedelk.database.internal.commons.DatabaseDriver;
import com.reedelk.database.internal.ddlexecute.DDLDefinitionStrategy;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicString;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

@EmbeddedDatabaseTest(engine = Engine.H2)
@ExtendWith(MockitoExtension.class)
class DDLExecuteTest {

    @Mock
    private ScriptEngineService mockScriptEngine;
    @Mock
    private FlowContext mockFlowContext;

    private Message testMessage = MessageBuilder.get().withText("test").build();

    private DDLExecute component = new DDLExecute();

    @BeforeEach
    void setUp() {
        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setConnectionURL("jdbc:h2:mem:" + DDLExecuteTest.class.getSimpleName());
        connectionConfiguration.setDatabaseDriver(DatabaseDriver.H2);
        component.setConnectionConfiguration(connectionConfiguration);
        component.dataSourceService = new DataSourceService();
        component.scriptEngine = mockScriptEngine;
    }

    @Test
    void shouldCorrectlyCreateTableFromDDL(@EmbeddedDatabase final DataSource dataSource) throws SQLException {
        // Given
        String createTable = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512))";
        lenient()
                .doReturn(Optional.of(createTable))
                .when(mockScriptEngine)
                .evaluate(any(DynamicString.class), any(FlowContext.class), any(Message.class));

        DynamicString string = DynamicString.from(createTable);
        component.setStrategy(DDLDefinitionStrategy.INLINE);
        component.setDdlDefinition(string);
        component.initialize();

        // When
        Message actual = component.apply(mockFlowContext, testMessage);

        // Then (if the table exists, it has been correctly added)
        ResultSet resultSet = dataSource.getConnection().createStatement().executeQuery("SELECT 1 FROM Customer LIMIT 1;");
        assertThat(resultSet).isNotNull();

        MessageAttributes attributes = actual.getAttributes();
        assertThat(attributes).containsEntry("ddl", "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512))");

        int result = actual.payload();
        assertThat(result).isEqualTo(0);
    }

    @Test
    void shouldIncludeInTheExceptionDDLStatementWhenExceptionThrown() {
        // Given
        String createTable = "CREATE NOT_VALID Customer(id INTEGER PRIMARY KEY, name VARCHAR(512))";
        lenient()
                .doReturn(Optional.of(createTable))
                .when(mockScriptEngine)
                .evaluate(any(DynamicString.class), any(FlowContext.class), any(Message.class));

        DynamicString string = DynamicString.from(createTable);
        component.setStrategy(DDLDefinitionStrategy.INLINE);
        component.setDdlDefinition(string);
        component.initialize();

        // When
        ESBException thrown = assertThrows(ESBException.class,
                () -> component.apply(mockFlowContext, testMessage));

        assertThat(thrown).isNotNull();
        assertThat(thrown).hasMessage("Could not execute DDL=[CREATE NOT_VALID Customer(id INTEGER PRIMARY KEY, name VARCHAR(512))]: Syntax error in SQL statement \"CREATE NOT_VALID[*] CUSTOMER(ID INTEGER PRIMARY KEY, NAME VARCHAR(512))\"; expected \"OR, FORCE, VIEW, ALIAS, SEQUENCE, USER, TRIGGER, ROLE, SCHEMA, CONSTANT, DOMAIN, TYPE, DATATYPE, AGGREGATE, LINKED, MEMORY, CACHED, LOCAL, GLOBAL, TEMP, TEMPORARY, TABLE, SYNONYM, PRIMARY, UNIQUE, HASH, SPATIAL, INDEX\"; SQL statement:\n" +
                "CREATE NOT_VALID Customer(id INTEGER PRIMARY KEY, name VARCHAR(512)) [42001-200]");
    }
}
