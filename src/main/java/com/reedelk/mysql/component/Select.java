package com.reedelk.mysql.component;

import com.reedelk.mysql.ConnectionConfiguration;
import com.reedelk.mysql.ConnectionPools;
import com.reedelk.mysql.DatabaseUtils;
import com.reedelk.mysql.DisposableResultSet;
import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.annotation.TabPlacementTop;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.script.dynamicmap.DynamicStringMap;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ESBComponent("SQL Select")
@Component(service = Select.class, scope = ServiceScope.PROTOTYPE)
public class Select implements ProcessorSync {

    @Property("Connection Configuration")
    private ConnectionConfiguration connectionConfiguration;
    @Property("SQL Query")
    private String query;
    @Property("Query Parameters")
    @TabPlacementTop
    private DynamicStringMap parametersMapping;

    @Reference
    private ConnectionPools pools;


    @Override
    public void initialize() {
        requireNotBlank(Select.class, query, "Query must not be null");
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = pools.getConnection(connectionConfiguration);
            statement = connection.createStatement();
            resultSet = statement.executeQuery(query);

            DisposableResultSet wrappedResultSet = new DisposableResultSet(connection, statement, resultSet);
            flowContext.register(wrappedResultSet);
            return MessageBuilder.get().withJavaObject(wrappedResultSet).build();
        } catch (Throwable exception) {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);
            throw new ESBException(exception);
        }
    }

    @Override
    public void dispose() {
        // TODO: This is wrong.
        // Every configuration is a datasource. If there are no more components
        // using that configuration, then we can safely close it otherwise not.
        // So it is: components -> Datasource
        pools.dispose();
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
    }

    public void setParametersMapping(DynamicStringMap parametersMapping) {
        this.parametersMapping = parametersMapping;
    }
}
