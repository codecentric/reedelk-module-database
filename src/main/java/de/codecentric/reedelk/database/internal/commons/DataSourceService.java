package de.codecentric.reedelk.database.internal.commons;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import de.codecentric.reedelk.database.component.ConnectionConfiguration;
import de.codecentric.reedelk.database.component.Select;
import de.codecentric.reedelk.runtime.api.component.Implementor;
import de.codecentric.reedelk.runtime.api.exception.PlatformException;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static de.codecentric.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotNull;
import static de.codecentric.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireTrue;
import static java.lang.String.format;
import static org.osgi.service.component.annotations.ServiceScope.SINGLETON;

@Component(service = DataSourceService.class, scope = SINGLETON)
public class DataSourceService {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceService.class);

    final Map<String, ComboPooledDataSource> CONFIG_ID_CONNECTION_POOL_MAP = new HashMap<>();
    final Map<String, List<de.codecentric.reedelk.runtime.api.component.Component>> CONFIG_ID_COMPONENT_MAP = new HashMap<>();

    public synchronized ComboPooledDataSource getDataSource(de.codecentric.reedelk.runtime.api.component.Component component, ConnectionConfiguration connectionConfiguration) {
        requireNotNull((Class<? extends Implementor>) Select.class, connectionConfiguration, "Connection configuration must be available");
        DatabaseDriver databaseDriverClass = connectionConfiguration.getDatabaseDriver();
        requireTrue(component.getClass(),
                IsDriverAvailable.of(databaseDriverClass),
                format("Driver '%s' not found. Make sure that the driver is inside {RUNTIME_HOME}/lib directory.", databaseDriverClass));

        String configId = connectionConfiguration.getId();

        if (!CONFIG_ID_CONNECTION_POOL_MAP.containsKey(configId)) {
            // We need to create the data source
            ComboPooledDataSource pooledDataSource = new ComboPooledDataSource();
            try {
                pooledDataSource.setDriverClass(databaseDriverClass.qualifiedName());
            } catch (Throwable exception) {
                throw new PlatformException(exception);
            }

            pooledDataSource.setJdbcUrl(connectionConfiguration.getConnectionURL());
            pooledDataSource.setUser(connectionConfiguration.getUsername());
            pooledDataSource.setPassword(connectionConfiguration.getPassword());
            Optional.ofNullable(connectionConfiguration.getMinPoolSize())
                    .ifPresent(pooledDataSource::setMinPoolSize);
            Optional.ofNullable(connectionConfiguration.getMaxPoolSize())
                    .ifPresent(pooledDataSource::setMaxPoolSize);
            Optional.ofNullable(connectionConfiguration.getAcquireIncrement())
                    .ifPresent(pooledDataSource::setAcquireIncrement);
            CONFIG_ID_CONNECTION_POOL_MAP.put(configId, pooledDataSource);
        }
        addComponentMapping(configId, component);
        return CONFIG_ID_CONNECTION_POOL_MAP.get(configId);

    }

    public synchronized void dispose(de.codecentric.reedelk.runtime.api.component.Component component, ConnectionConfiguration connectionConfiguration) {
        if (CONFIG_ID_COMPONENT_MAP.containsKey(connectionConfiguration.getId())) {
            List<de.codecentric.reedelk.runtime.api.component.Component> components = CONFIG_ID_COMPONENT_MAP.get(connectionConfiguration.getId());
            components.remove(component);
            if (components.isEmpty()) {
                // If there are not components using this data source, we
                // can close it since it is not in use anymore.
                CONFIG_ID_COMPONENT_MAP.remove(connectionConfiguration.getId());
                ComboPooledDataSource toClose = CONFIG_ID_CONNECTION_POOL_MAP.remove(connectionConfiguration.getId());
                silentlyClose(toClose);
            }
        }
    }

    public synchronized void dispose() {
        CONFIG_ID_CONNECTION_POOL_MAP.forEach((configurationId, comboPooledDataSource) -> silentlyClose(comboPooledDataSource));
        CONFIG_ID_CONNECTION_POOL_MAP.clear();
        CONFIG_ID_COMPONENT_MAP.clear();
    }

    void silentlyClose(ComboPooledDataSource toClose) {
        try {
            if (toClose != null) {
                toClose.close();
            }
        } catch (Exception exception) {
            logger.warn("Could not close pooled data source", exception);
        }
    }

    private void addComponentMapping(String configId, de.codecentric.reedelk.runtime.api.component.Component component) {
        if (CONFIG_ID_COMPONENT_MAP.containsKey(configId)) {
            CONFIG_ID_COMPONENT_MAP.get(configId).add(component);
        } else {
            List<de.codecentric.reedelk.runtime.api.component.Component> components = new ArrayList<>();
            components.add(component);
            CONFIG_ID_COMPONENT_MAP.put(configId, components);
        }
    }
}
