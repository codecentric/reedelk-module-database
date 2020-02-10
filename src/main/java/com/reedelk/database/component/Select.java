package com.reedelk.database.component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.database.commons.*;
import com.reedelk.database.configuration.ConnectionConfiguration;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
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

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ESBComponent("SQL Select")
@Component(service = Select.class, scope = ServiceScope.PROTOTYPE)
public class Select implements ProcessorSync {

    @Property("Connection")
    @PropertyInfo("Data source configuration to be used by this query. " +
            "Shared configurations use the same connection pool.")
    private ConnectionConfiguration connectionConfiguration;

    @Property("Select Query")
    @Hint("SELECT * FROM orders WHERE name LIKE :name")
    @PropertyInfo("The <b>select</b> query to be executed on the database with the given Data Source connection. " +
            "The query might contain parameters which will be filled from the expressions defined in" +
            "the parameters mapping configuration below. Examples:<br>" +
            "<ul>" +
            "<li>SELECT * FROM orders WHERE name = 'John' AND surname = 'Doe'</li>" +
            "<li>SELECT * FROM orders WHERE name LIKE :name AND surname = :surname</li>" +
            "</ul>")
    private String query;

    @Property("Query Variables Mappings")
    @TabPlacementTop
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

            // TODO: Throw exception if query is not null need to put in the exception the real query!
            //  it would be much easier to debug if the executed query is logged.
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);
            throw new ESBException(exception);
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

        // TODO: The message should contain in the attributes the executed SELECT.
        return MessageBuilder.get()
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