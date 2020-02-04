package com.reedelk.database.component;

import com.reedelk.database.ConnectionConfiguration;
import com.reedelk.database.ConnectionPools;
import com.reedelk.database.DatabaseUtils;
import com.reedelk.database.utils.IsDriverAvailable;
import com.reedelk.database.utils.QueryStatementTemplate;
import com.reedelk.runtime.api.annotation.ESBComponent;
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.*;
import static java.lang.String.format;

@ESBComponent("SQL Update")
@Component(service = Update.class, scope = ServiceScope.PROTOTYPE)
public class Update implements ProcessorSync {

    @Property("Connection")
    private ConnectionConfiguration connectionConfiguration;
    @Property("Update Query")
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
        requireNotBlank(Insert.class, query, "Update query is not defined");
        requireNotNull(Insert.class, connectionConfiguration, "Connection configuration must be available");
        String driverClass = connectionConfiguration.getDriverClass();
        requireTrue(Insert.class,
                IsDriverAvailable.of(driverClass),
                format("Driver '%s' not found. Make sure that the driver is inside {RUNTIME_HOME}/lib directory.", driverClass));
        queryStatement = new QueryStatementTemplate(query);
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = pools.getConnection(connectionConfiguration);
            statement = connection.createStatement();

            Map<String, Object> evaluatedMap = scriptEngine.evaluate(parametersMapping, flowContext, message);
            String realQuery = queryStatement.replace(evaluatedMap);

            int rowCount = statement.executeUpdate(realQuery);

            return MessageBuilder.get().withJavaObject(rowCount).build();

        } catch (Throwable exception) {
            throw new ESBException(exception);
        } finally {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);
        }
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