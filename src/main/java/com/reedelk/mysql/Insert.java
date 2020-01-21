package com.reedelk.mysql;

import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotBlank;

@ESBComponent("SQL Insert")
@Component(service = Insert.class, scope = ServiceScope.PROTOTYPE)
public class Insert implements ProcessorSync {

    @Property("SQL Query")
    private String query;
    @Property("Connection Configuration")
    private ConnectionConfiguration connectionConfiguration;

    @Reference
    private ConnectionPools pools;


    @Override
    public void initialize() {
        requireNotBlank(Insert.class, query, "Query must not be null");
    }

    @Override
    public Message apply(Message message, FlowContext flowContext) {
        try (Connection connection = pools.getConnection(connectionConfiguration);
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                String id = resultSet.getString("id");
                String name = resultSet.getString("name");
                System.out.println(id + " name: " + name);
            }
            return MessageBuilder.get().build();
        } catch (Throwable e) {
            throw new ESBException(e);
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
}
