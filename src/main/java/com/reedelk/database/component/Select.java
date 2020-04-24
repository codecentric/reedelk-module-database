package com.reedelk.database.component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.database.internal.commons.*;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ImmutableMap;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.DataRow;
import com.reedelk.runtime.api.message.content.TypedPublisher;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;
import reactor.core.publisher.Flux;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;

import static com.reedelk.database.internal.commons.Messages.Select.QUERY_EXECUTE_ERROR;
import static com.reedelk.database.internal.commons.Messages.Select.QUERY_EXECUTE_ERROR_WITH_QUERY;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
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
            "The query might contain parameters which will be filled from the expressions defined in " +
            "the parameters mapping configuration. below.")
    private String query;

    @Property("Query Parameter Mappings")
    @TabGroup("Query Parameter Mappings")
    @KeyName("Query Parameter Name")
    @ValueName("Query Parameter Value")
    @Example("name > <code>message.payload()</code>")
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

    @SuppressWarnings("rawtypes")
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
            throw new PlatformException(errorMessage, exception);
        }

        DisposableResultSet disposableResultSet = new DisposableResultSet(connection, statement, resultSet);
        flowContext.register(disposableResultSet);

        // TODO: This is wrong the result set should not contain all the attributes, the attributes
        //   are the same for all the rows it does not make sense to have the attributes for each row.
        //  the DataRow object should just not contain attributes. Also from the data Row I want to be able
        //  to get the value from the ID and from the Column Name. Eg item['MY_COLUMN'] also the output
        //  from the DB should be possible to write to CSV.
        TypedPublisher<DataRow> result = createResultStream(disposableResultSet);

        Map<String, Serializable> attributes = ImmutableMap.of(DatabaseAttribute.QUERY, realQuery);

        return MessageBuilder.get(Select.class)
                .withTypedPublisher(result)
                .attributes(attributes)
                .build();
    }

    @Override
    public void dispose() {
        this.dataSourceService.dispose(this, connectionConfiguration);
        this.dataSource = null;
        this.queryStatement = null;
    }

    public void setConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
    }

    public void setParametersMapping(DynamicObjectMap parametersMapping) {
        this.parametersMapping = parametersMapping;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    @SuppressWarnings({"rawtypes"})
    private TypedPublisher<DataRow> createResultStream(DisposableResultSet disposableResultSet) {
        return TypedPublisher.from(Flux.create(sink -> {
            try {
                ResultSetMetaData metaData = disposableResultSet.getMetaData();
                JDBCMetadata jdbcMetadata = JDBCMetadata.from(metaData);
                while (disposableResultSet.next()) {
                    DataRow<Serializable> row = ResultSetConverter.convertRow(jdbcMetadata, disposableResultSet);
                    sink.next(row);
                }
                sink.complete();
            } catch (Throwable exception) {
                sink.error(exception);
            }
        }), DataRow.class);
    }
}
