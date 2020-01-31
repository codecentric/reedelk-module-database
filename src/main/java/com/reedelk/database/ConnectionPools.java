package com.reedelk.database;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.runtime.api.exception.ESBException;
import org.osgi.service.component.annotations.Component;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.osgi.service.component.annotations.ServiceScope.SINGLETON;

@Component(service = ConnectionPools.class, scope = SINGLETON)
public class ConnectionPools {

    private final Map<String, ComboPooledDataSource> CONFIG_ID_CONNECTION_POOL_MAP = new HashMap<>();

    public Connection getConnection(ConnectionConfiguration connectionConfiguration) {
        if (CONFIG_ID_CONNECTION_POOL_MAP.containsKey(connectionConfiguration.getId())) {
            return getConnectionInternal(connectionConfiguration);
        }

        synchronized (CONFIG_ID_CONNECTION_POOL_MAP) {
            if (CONFIG_ID_CONNECTION_POOL_MAP.containsKey(connectionConfiguration.getId())) {
                return getConnectionInternal(connectionConfiguration);
            }
            // Otherwise create
            ComboPooledDataSource cpds = new ComboPooledDataSource();
            try {
                cpds.setDriverClass(connectionConfiguration.getDriverClass());
                cpds.setJdbcUrl(connectionConfiguration.getConnectionURL());
                cpds.setUser(connectionConfiguration.getUsername());
                cpds.setPassword(connectionConfiguration.getPassword());
                CONFIG_ID_CONNECTION_POOL_MAP.put(connectionConfiguration.getId(), cpds);

                return cpds.getConnection();
            } catch (Throwable exception) {
                throw new ESBException(exception);
            }
        }
    }

    private Connection getConnectionInternal(ConnectionConfiguration connectionConfiguration) {
        try {
            return CONFIG_ID_CONNECTION_POOL_MAP.get(connectionConfiguration.getId()).getConnection();
        } catch (SQLException exception) {
            throw new ESBException(exception);
        }
    }

    public void dispose() {
        CONFIG_ID_CONNECTION_POOL_MAP.forEach((configurationId, comboPooledDataSource) -> comboPooledDataSource.close());
        CONFIG_ID_CONNECTION_POOL_MAP.clear();
    }
}
