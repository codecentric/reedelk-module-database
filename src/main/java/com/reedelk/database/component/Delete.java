package com.reedelk.database.component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.database.internal.attribute.DatabaseAttributes;
import com.reedelk.database.internal.commons.DataSourceService;
import com.reedelk.database.internal.commons.DatabaseUtils;
import com.reedelk.database.internal.commons.QueryStatementTemplate;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import static com.reedelk.database.internal.commons.Messages.Delete.QUERY_EXECUTE_ERROR;
import static com.reedelk.database.internal.commons.Messages.Delete.QUERY_EXECUTE_ERROR_WITH_QUERY;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.StackTraceUtils.rootCauseMessageOf;

@ModuleComponent("SQL Delete")
@ComponentOutput(
        attributes = DatabaseAttributes.class,
        payload = int.class,
        description = "The number of rows deleted from the database.")
@ComponentInput(
        payload = Object.class,
        description = "The input payload is used to evaluate the expressions bound to the query parameters mappings.")
@Description("Executes a DELETE SQL statement on the configured data source connection. Supported databases and drivers: H2 (org.h2.Driver), MySQL (com.mysql.cj.jdbc.Driver), Oracle (oracle.jdbc.Driver), PostgreSQL (org.postgresql.Driver).")
@Component(service = Delete.class, scope = ServiceScope.PROTOTYPE)
public class Delete implements ProcessorSync {

    @DialogTitle("Data Source Configuration")
    @Property("Connection")
    @Description("Data source configuration to be used by this query. " +
            "Shared configurations use the same connection pool.")
    private ConnectionConfiguration connection;

    @Property("Delete Query")
    @Example("<ul>" +
            "<li>DELETE FROM orders WHERE id = 1</li>" +
            "<li>DELETE FROM orders WHERE name LIKE 'item%'</li>" +
            "<li>DELETE * FROM employees WHERE employee_country = :country</li>" +
            "</ul>")
    @Hint("DELETE FROM orders WHERE id = 1")
    @Description("The <b>delete</b> query to be executed on the database with the given Data Source connection. " +
            "The query might contain parameters which will be filled from the expressions defined in " +
            "the parameters mapping configuration below.")
    private String query;

    @Property("Query Parameter Mappings")
    @TabGroup("Query Parameter Mappings")
    @KeyName("Query Parameter Name")
    @ValueName("Query Parameter Value")
    @Example("id > <code>message.payload()</code>")
    @Description("Mapping of delete query parameters > values. Query parameters will be evaluated and replaced each time before the query is executed.")
    private DynamicObjectMap parametersMapping = DynamicObjectMap.empty();

    @Reference
    DataSourceService dataSourceService;
    @Reference
    ScriptEngineService scriptEngine;

    private QueryStatementTemplate queryStatement;

    private ComboPooledDataSource dataSource;

    @Override
    public void initialize() {
        requireNotBlank(Insert.class, query, "Delete query is not defined");
        dataSource = dataSourceService.getDataSource(this, connection);
        queryStatement = new QueryStatementTemplate(query);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        String realQuery = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();

            Map<String, Object> evaluatedMap = scriptEngine.evaluate(parametersMapping, flowContext, message);
            realQuery = queryStatement.replace(evaluatedMap);

            int rowCount = statement.executeUpdate(realQuery);

            MessageAttributes attributes = new DatabaseAttributes(realQuery);

            return MessageBuilder.get(Delete.class)
                    .withJavaObject(rowCount)
                    .attributes(attributes)
                    .build();

        } catch (Throwable exception) {
            String errorMessage = Optional.ofNullable(realQuery)
                    .map(query -> QUERY_EXECUTE_ERROR_WITH_QUERY.format(query, rootCauseMessageOf(exception)))
                    .orElse(QUERY_EXECUTE_ERROR.format(rootCauseMessageOf(exception)));
            throw new PlatformException(errorMessage, exception);

        } finally {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);
        }
    }

    @Override
    public void dispose() {
        this.dataSourceService.dispose(this, connection);
        this.dataSource = null;
        this.queryStatement = null;
    }

    public void setParametersMapping(DynamicObjectMap parametersMapping) {
        this.parametersMapping = parametersMapping;
    }

    public void setConnection(ConnectionConfiguration connection) {
        this.connection = connection;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
