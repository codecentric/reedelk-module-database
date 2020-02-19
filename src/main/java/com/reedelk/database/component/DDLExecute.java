package com.reedelk.database.component;

import com.reedelk.database.commons.DataSourceService;
import com.reedelk.database.configuration.ConnectionConfiguration;
import com.reedelk.database.configuration.DDLDefinitionStrategy;
import com.reedelk.database.ddlexecute.ExecutionStrategy;
import com.reedelk.database.ddlexecute.ExecutionStrategyBuilder;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.resource.ResourceText;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicString;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

import javax.sql.DataSource;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotNull;

@ModuleComponent(
        name = "DDL Execute",
        description = "Executes the given DDL statement/s on the configured data source connection. " +
                "This component can be used to create/drop/alter/rename database tables. Supported databases " +
                "and drivers: H2 (org.h2.Driver), MySQL (com.mysql.cj.jdbc.Driver), Oracle (oracle.jdbc.Driver), PostgreSQL (org.postgresql.Driver). " +
                "The database drivers libraries must be present in the <b>{RUNTIME_HOME}/lib</b> directory.")
@Component(service = DDLExecute.class, scope = ServiceScope.PROTOTYPE)
public class DDLExecute implements ProcessorSync {

    @Property("Connection")
    @PropertyDescription("Data source configuration where the DDL statements will be executed on. " +
            "Shared configurations use the same connection pool.")
    private ConnectionConfiguration connectionConfiguration;

    @Example("FROM_FILE")
    @InitValue("INLINE")
    @DefaultRenameMe("INLINE")
    @Property("Strategy")
    @PropertyDescription("Execution strategy for this DDL. If <b>INLINE</b> then a static or dynamic inline statement is executed from the given <i>ddlDefinition</i> property," +
            " otherwise if <b>FROM_FILE</b> DDL statements are executed from the given <i>ddlFile</i> local project's file.")
    private DDLDefinitionStrategy strategy;

    @Example("<ul>" +
            "<li><code>CREATE TABLE person (id INT, name VARCHAR(255), surname VARCHAR(255), address VARCHAR(255), city VARCHAR(255))</code></li>" +
            "<li><code>DROP TABLE person</code></li>" +
            "</ul>")
    @Hint("CREATE TABLE person (id INT, name VARCHAR(255), surname VARCHAR(255), address VARCHAR(255), city VARCHAR(255))")
    @When(propertyName = "strategy", propertyValue = "INLINE")
    @Property("DDL Definition")
    @PropertyDescription("Sets the DDL definition to be executed by this component. The DDL definition might be a static or dynamic value.")
    private DynamicString ddlDefinition;

    @Example("assets/create_table_company.sql")
    @When(propertyName = "strategy", propertyValue = "FROM_FILE")
    @Property("DDL File")
    @PropertyDescription("Sets the file path in the project's resources directory containing the DDL statements to be executed when the strategy is <b>FROM_FILE</b>.")
    private ResourceText ddlFile;

    @Reference
    DataSourceService dataSourceService;
    @Reference
    ScriptEngineService scriptEngine;

    private ExecutionStrategy executionStrategy;

    private DataSource dataSource;

    @Override
    public void initialize() {
        requireNotNull(DDLExecute.class, connectionConfiguration, "Connection configuration must be defined.");
        dataSource = dataSourceService.getDataSource(this, connectionConfiguration);
        executionStrategy = ExecutionStrategyBuilder.get()
                .with(strategy)
                .with(ddlFile)
                .with(dataSource)
                .with(ddlDefinition)
                .with(scriptEngine)
                .build();

    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        return executionStrategy.execute(flowContext, message);
    }

    @Override
    public void dispose() {
        this.dataSourceService.dispose(this, connectionConfiguration);
        this.ddlFile = null;
        this.dataSource = null;
        this.ddlDefinition = null;
    }

    public void setConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
        this.connectionConfiguration = connectionConfiguration;
    }

    public void setStrategy(DDLDefinitionStrategy strategy) {
        this.strategy = strategy;
    }

    public void setDdlDefinition(DynamicString ddlDefinition) {
        this.ddlDefinition = ddlDefinition;
    }

    public void setDdlFile(ResourceText ddlFile) {
        this.ddlFile = ddlFile;
    }
}
