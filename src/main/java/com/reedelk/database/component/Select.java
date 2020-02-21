package com.reedelk.database.component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.database.commons.*;
import com.reedelk.database.configuration.ConnectionConfiguration;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.DefaultMessageAttributes;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.ResultRow;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import static com.reedelk.database.commons.Messages.Select.QUERY_EXECUTE_ERROR;
import static com.reedelk.database.commons.Messages.Select.QUERY_EXECUTE_ERROR_WITH_QUERY;
import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;
import static com.reedelk.runtime.api.commons.StackTraceUtils.rootCauseMessageOf;

@ModuleComponent("SQL Select")
@Description("Executes a SELECT SQL statement on the configured data source connection. Supported databases and drivers: H2 (org.h2.Driver), MySQL (com.mysql.cj.jdbc.Driver), Oracle (oracle.jdbc.Driver), PostgreSQL (org.postgresql.Driver).")
@Component(service = Select.class, scope = ServiceScope.PROTOTYPE)
public class Select implements ProcessorSync {

    @Property("Connection")
    @Description("Data source configuration to be used by this query. " +
            "Shared configurations use the same connection pool.")
    private ConnectionConfiguration connectionConfiguration;

    @Example("<ul>" +
            "<li><code>SELECT * FROM orders WHERE name = 'John' AND surname = 'Doe'</code></li>" +
            "<li><code>SELECT * FROM orders WHERE name LIKE :name AND surname = :surname</code></li>" +
            "</ul>")
    @Property("Select Query")
    @Hint("SELECT * FROM orders WHERE name LIKE :name")
    @Description("The <b>select</b> query to be executed on the database with the given Data Source connection. " +
            "The query might contain parameters which will be filled from the expressions defined in" +
            "the parameters mapping configuration. below.")
    private String query;

    @Example("name > <code>message.payload()</code>")
    @TabPlacementTop
    @Property("Query Parameters Mappings")
    @Description("Mapping of select query parameters > values. Query parameters will be evaluated and replaced each time before the query is executed.")
    private DynamicObjectMap parametersMapping = DynamicObjectMap.empty();

    @Reference
    DataSourceService dataSourceService;
    @Reference
    ScriptEngineService scriptEngine;

    private ComboPooledDataSource dataSource;
    private QueryStatementTemplate queryStatement;

    @Override
    public void initialize() {
        requireNotBlank(Select.class, query, "Select query is not defined");
        dataSource = dataSourceService.getDataSource(this, connectionConfiguration);
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

            resultSet = statement.executeQuery(realQuery);

        } catch (Throwable exception) {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);

            String errorMessage = Optional.ofNullable(realQuery)
                    .map(query -> QUERY_EXECUTE_ERROR_WITH_QUERY.format(query, rootCauseMessageOf(exception)))
                    .orElse(QUERY_EXECUTE_ERROR.format(rootCauseMessageOf(exception)));
            throw new ESBException(errorMessage, exception);
        }

        DisposableResultSet disposableResultSet = new DisposableResultSet(connection, statement, resultSet);
        flowContext.register(disposableResultSet);

        Flux<ResultRow> result = Flux.create(sink -> {
            try {
                ResultSetMetaData metaData = disposableResultSet.getMetaData();
                while (disposableResultSet.next()) {
                    ResultRow row = ResultSetConverter.convertRow(metaData, disposableResultSet);
                    sink.next(row);
                }
                sink.complete();
            } catch (Throwable exception) {
                sink.error(exception);
            }
        });

        MessageAttributes attributes = new DefaultMessageAttributes(Select.class,
                ImmutableMap.of(DatabaseAttribute.QUERY, realQuery));
        return MessageBuilder.get()
                .attributes(attributes)
                .withStream(result, ResultRow.class)
                .build();
    }

    @Override
    public void dispose() {
        this.dataSourceService.dispose(this, connectionConfiguration);
        this.dataSource = null;
        this.queryStatement = null;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
    }

    public void setParametersMapping(DynamicObjectMap parametersMapping) {
        this.parametersMapping = parametersMapping;
    }
}
