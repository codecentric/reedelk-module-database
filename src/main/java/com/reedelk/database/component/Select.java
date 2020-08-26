package com.reedelk.database.component;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.database.internal.attribute.DatabaseAttributes;
import com.reedelk.database.internal.attribute.SelectAttributes;
import com.reedelk.database.internal.commons.*;
import com.reedelk.database.internal.exception.SelectException;
import com.reedelk.database.internal.type.DatabaseRow;
import com.reedelk.database.internal.type.ListOfDatabaseRow;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.TypedPublisher;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicmap.DynamicObjectMap;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;
import reactor.core.publisher.Flux;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.reedelk.database.internal.commons.Messages.Select.*;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.StackTraceUtils.rootCauseMessageOf;

@ModuleComponent("SQL Select")
@ComponentOutput(
        attributes = DatabaseAttributes.class,
        payload = ListOfDatabaseRow.class,
        description = "A list of database rows.")
@ComponentInput(
        payload = Object.class,
        description = "The input payload is used to evaluate the expressions bound to the query parameters mappings.")
@Description("Executes a SELECT SQL statement on the configured data source connection. Supported databases and drivers: H2 (org.h2.Driver), MySQL (com.mysql.cj.jdbc.Driver), Oracle (oracle.jdbc.Driver), PostgreSQL (org.postgresql.Driver).")
@Component(service = Select.class, scope = ServiceScope.PROTOTYPE)
public class Select implements ProcessorSync {

    @DialogTitle("Data Source Configuration")
    @Property("Connection")
    @Description("Data source configuration to be used by this query. " +
            "Shared configurations use the same connection pool.")
    private ConnectionConfiguration connection;

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

            resultSet = statement.executeQuery(realQuery);

        } catch (Throwable exception) {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);

            String error = Optional.ofNullable(realQuery)
                    .map(query -> QUERY_EXECUTE_ERROR_WITH_QUERY.format(query, rootCauseMessageOf(exception)))
                    .orElse(QUERY_EXECUTE_ERROR.format(rootCauseMessageOf(exception)));
            throw new SelectException(error, exception);
        }

        DisposableResultSet disposableResultSet = new DisposableResultSet(connection, statement, resultSet);
        flowContext.register(disposableResultSet);

        ResultSetMetaData metaData;
        try {
            metaData = disposableResultSet.getMetaData();
        } catch (SQLException exception) {
            String error = METADATA_FETCH_ERROR.format(
                    exception.getErrorCode(),
                    exception.getSQLState(),
                    exception.getMessage());
            throw new SelectException(error, exception);
        }

        List<Integer> columnTypes = MetadataUtils.getColumnType(metaData);
        Map<String, Integer> columnNameIndexMap = MetadataUtils.getColumnNameIndexMap(metaData);
        Map<Integer, String> columnIndexNameMap = MetadataUtils.getColumnIndexNameMap(metaData);

        TypedPublisher<DatabaseRow> result =
                createResultStream(metaData, disposableResultSet, columnNameIndexMap, columnIndexNameMap);

        SelectAttributes selectAttributes = new SelectAttributes(query, columnTypes);

        return MessageBuilder.get(Select.class)
                .withTypedPublisher(result)
                .attributes(selectAttributes)
                .build();
    }

    @Override
    public void dispose() {
        this.dataSourceService.dispose(this, connection);
        this.dataSource = null;
        this.queryStatement = null;
    }

    public void setConnection(ConnectionConfiguration connection) {
        this.connection = connection;
    }

    public void setParametersMapping(DynamicObjectMap parametersMapping) {
        this.parametersMapping = parametersMapping;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    private TypedPublisher<DatabaseRow> createResultStream(
            ResultSetMetaData metaData,
            DisposableResultSet disposableResultSet,
            Map<String, Integer> columnNameIndexMap,
            Map<Integer, String> columnIndexNameMap) {

        return TypedPublisher.from(Flux.create(sink -> {
            try {

                while (disposableResultSet.next()) {
                    DatabaseRow row = DatabaseRowConverter.convert(
                            metaData,
                            disposableResultSet,
                            columnNameIndexMap,
                            columnIndexNameMap);
                    sink.next(row);
                }

                sink.complete();

            } catch (Throwable exception) {
                sink.error(exception);
            }

        }), DatabaseRow.class);
    }
}
