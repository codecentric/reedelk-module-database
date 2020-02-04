package com.reedelk.database.component;

import com.reedelk.database.*;
import com.reedelk.database.utils.IsDriverAvailable;
import com.reedelk.database.utils.QueryStatementTemplate;
import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.annotation.Hint;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.annotation.TabPlacementTop;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.*;
import static java.lang.String.format;

@ESBComponent("SQL Select")
@Component(service = Select.class, scope = ServiceScope.PROTOTYPE)
public class Select implements ProcessorSync {

    @Property("Connection")
    private ConnectionConfiguration connectionConfiguration;
    @Property("Select Query")
    @Hint("SELECT * FROM orders WHERE  name LIKE :name")
    private String query;
    @Property("Query Variables Mappings")
    @TabPlacementTop
    private DynamicObjectMap parametersMapping = DynamicObjectMap.empty();

    @Reference
    private ConnectionPools pools;
    @Reference
    private ScriptEngineService scriptEngine;

    private QueryStatementTemplate queryStatement;

    @Override
    public void initialize() {
        requireNotBlank(Select.class, query, "Select query is not defined");
        requireNotNull(Select.class, connectionConfiguration, "Connection configuration must be available");
        String driverClass = connectionConfiguration.getDriverClass();
        requireTrue(Select.class,
                IsDriverAvailable.of(driverClass),
                format("Driver '%s' not found. Make sure that the driver is inside {RUNTIME_HOME}/lib directory.", driverClass));
        queryStatement = new QueryStatementTemplate(query);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {

        // TODO: This is stream based version.
        Flux<ResultRow> result = Flux.create(sink -> {

            Connection connection = null;
            Statement statement = null;
            ResultSet resultSet = null;
            try {
                connection = pools.getConnection(connectionConfiguration);
                statement = connection.createStatement();

                Map<String, Object> evaluatedMap = scriptEngine.evaluate(parametersMapping, flowContext, message);
                String realQuery = queryStatement.replace(evaluatedMap);

                resultSet = statement.executeQuery(realQuery);

                // A disposable result set.
                // TODO: IF IT IS A STREAM, then we must do it later.
                DisposableResultSet wrappedResultSet = new DisposableResultSet(connection, statement, resultSet);
                // flowContext.register(wrappedResultSet);

                try {
                    ResultSetMetaData metaData = wrappedResultSet.getMetaData();
                    while (wrappedResultSet.next()) {
                        ResultRow resultRow = ResultSetConverter.convertRow(metaData, wrappedResultSet);
                        sink.next(resultRow);
                    }
                    sink.complete();
                } catch (Throwable exception) {
                    exception.printStackTrace();
                    sink.error(exception);
                }

            } catch (Throwable exception) {
                DatabaseUtils.closeSilently(resultSet);
                DatabaseUtils.closeSilently(statement);
                DatabaseUtils.closeSilently(connection);
                throw new ESBException(exception);
            } finally {
                DatabaseUtils.closeSilently(resultSet);
                DatabaseUtils.closeSilently(statement);
                DatabaseUtils.closeSilently(connection);
            }
        });

        return MessageBuilder.get()
                .withStream(result.map(new Function<ResultRow, String>() {
                    @Override
                    public String apply(ResultRow resultRow) {
                        return (String)resultRow.get(1);
                    }
                }), String.class)
                .build();
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