package com.reedelk.database.commons;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.reedelk.database.configuration.ConnectionConfiguration;
import com.reedelk.runtime.api.component.Component;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.zapodot.junit.db.annotations.EmbeddedDatabaseTest;
import org.zapodot.junit.db.common.Engine;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@EmbeddedDatabaseTest(
        engine = Engine.H2,
        initialSqls = "CREATE TABLE Customer(id INTEGER PRIMARY KEY, name VARCHAR(512)); "
                + "INSERT INTO CUSTOMER(id, name) VALUES (1, 'John Doe');"
                + "INSERT INTO CUSTOMER(id, name) VALUES (2, 'Mark Anton');"
)
class DataSourceServiceTest {

    private DataSourceService service;

    @BeforeEach
    void setUp() {
        service = spy(new DataSourceService());
    }

    @Test
    void shouldCreateDataSourceCorrectlyAndPopulateConfigMaps() {
        // Given
        String connectionId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = newConfig(connectionId);

        Component component = new TestComponent();

        // When
        ComboPooledDataSource dataSource =
                service.getDataSource(component, configuration);

        // Then
        assertThat(dataSource).isNotNull();
        assertThat(service.CONFIG_ID_CONNECTION_POOL_MAP).containsEntry(connectionId, dataSource);
        assertThat(service.CONFIG_ID_COMPONENT_MAP).containsEntry(connectionId, Collections.singletonList(component));
    }

    @Test
    void shouldReuseExistingDataSourceForConfigAndPopulateConfigComponentMap() {
        // Given
        String connectionId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = newConfig(connectionId);

        Component component1 = new TestComponent();
        service.getDataSource(component1, configuration);

        // Second component re-using the same configuration.
        Component component2 = new TestComponent();

        // When
        ComboPooledDataSource dataSource =
                service.getDataSource(component2, configuration);

        // Then
        assertThat(dataSource).isNotNull();
        assertThat(service.CONFIG_ID_CONNECTION_POOL_MAP).containsEntry(connectionId, dataSource);
        assertThat(service.CONFIG_ID_COMPONENT_MAP).containsKey(connectionId);

        List<Component> componentsUsingConnectionConfig =
                service.CONFIG_ID_COMPONENT_MAP.get(connectionId);
        assertThat(componentsUsingConnectionConfig).containsExactlyInAnyOrder(component1, component2);
    }

    @Test
    void shouldRemoveComponentFromListOfConfigIdComponentMap() {
        // Given
        String connectionId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = newConfig(connectionId);

        Component component1 = new TestComponent();
        service.getDataSource(component1, configuration);

        Component component2 = new TestComponent();
        service.getDataSource(component2, configuration);

        // When
        service.dispose(component1, configuration);

        // Then
        List<Component> componentsUsingConnectionConfig =
                service.CONFIG_ID_COMPONENT_MAP.get(connectionId);
        assertThat(componentsUsingConnectionConfig).containsOnly(component2);
    }

    @Test
    void shouldCloseDataSourceWhenThereAreNotComponentsUsingIt() {
        // Given
        String connectionId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = newConfig(connectionId);

        Component component1 = new TestComponent();
        ComboPooledDataSource dataSource1 = service.getDataSource(component1, configuration);

        Component component2 = new TestComponent();
        ComboPooledDataSource dataSource2 = service.getDataSource(component2, configuration);

        // When
        service.dispose(component1, configuration);
        service.dispose(component2, configuration);

        // Then
        assertThat(dataSource1).isEqualTo(dataSource2);

        assertThat(service.CONFIG_ID_COMPONENT_MAP).doesNotContainKeys(connectionId);
        assertThat(service.CONFIG_ID_CONNECTION_POOL_MAP).doesNotContainKeys(connectionId);

        verify(service).silentlyClose(dataSource1);
    }

    @Test
    void shouldNotCloseDataSourceWhenThereAreOtherComponentsUsingId() {
        // Given
        String connectionId = UUID.randomUUID().toString();
        ConnectionConfiguration configuration = newConfig(connectionId);

        Component component1 = new TestComponent();
        ComboPooledDataSource dataSource = service.getDataSource(component1, configuration);

        Component component2 = new TestComponent();
        service.getDataSource(component2, configuration);

        // When
        service.dispose(component1, configuration);

        // Then
        assertThat(service.CONFIG_ID_COMPONENT_MAP).containsKey(connectionId);
        assertThat(service.CONFIG_ID_CONNECTION_POOL_MAP).containsKey(connectionId);

        assertThat(service.CONFIG_ID_COMPONENT_MAP).containsEntry(connectionId, Collections.singletonList(component2));
        assertThat(service.CONFIG_ID_CONNECTION_POOL_MAP).containsEntry(connectionId, dataSource);

        verify(service, never()).silentlyClose(dataSource);
    }

    @Test
    void shouldCloseAllDataSourcesWhenServiceDisposed() {
        // Given
        String connectionId1 = UUID.randomUUID().toString();
        ConnectionConfiguration configuration1 = newConfig(connectionId1);

        String connectionId2 = UUID.randomUUID().toString();
        ConnectionConfiguration configuration2 = newConfig(connectionId2);

        Component component1 = new TestComponent();
        ComboPooledDataSource dataSource1 = service.getDataSource(component1, configuration1);

        Component component2 = new TestComponent();
        ComboPooledDataSource dataSource2 = service.getDataSource(component2, configuration2);

        // When
        service.dispose();

        // Then
        assertThat(service.CONFIG_ID_COMPONENT_MAP).isEmpty();
        assertThat(service.CONFIG_ID_CONNECTION_POOL_MAP).isEmpty();

        verify(service).silentlyClose(dataSource1);
        verify(service).silentlyClose(dataSource2);
    }

    private ConnectionConfiguration newConfig(String connectionId) {
        ConnectionConfiguration configuration = new ConnectionConfiguration();
        configuration.setPassword("mypass");
        configuration.setUsername("myuser");
        configuration.setConnectionURL("jdbc:h2:mem:" + DataSourceServiceTest.class.getSimpleName());
        configuration.setDatabaseDriver(DatabaseDriver.H2);
        configuration.setId(connectionId);
        return configuration;
    }

    private static class TestComponent implements ProcessorSync {
        @Override
        public Message apply(FlowContext flowContext, Message message) {
            throw new UnsupportedOperationException("Not supposed to be called");
        }
    }
}