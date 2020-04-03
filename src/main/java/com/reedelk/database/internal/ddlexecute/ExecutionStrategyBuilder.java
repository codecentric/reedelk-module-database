package com.reedelk.database.internal.ddlexecute;

import com.reedelk.runtime.api.resource.ResourceText;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicString;

import javax.sql.DataSource;

public class ExecutionStrategyBuilder {

    private DDLDefinitionStrategy strategy;
    private DynamicString ddlDefinition;
    private ResourceText ddlFile;
    private DataSource dataSource;
    private ScriptEngineService scriptEngine;

    private ExecutionStrategyBuilder() {
    }

    public static ExecutionStrategyBuilder get() {
        return new ExecutionStrategyBuilder();
    }

    public ExecutionStrategyBuilder with(ResourceText ddlFile) {
        this.ddlFile = ddlFile;
        return this;
    }

    public ExecutionStrategyBuilder with(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    public ExecutionStrategyBuilder with(DynamicString ddlDefinition) {
        this.ddlDefinition = ddlDefinition;
        return this;
    }

    public ExecutionStrategyBuilder with(DDLDefinitionStrategy strategy) {
        this.strategy = strategy;
        return this;
    }

    public ExecutionStrategyBuilder with(ScriptEngineService scriptEngine) {
        this.scriptEngine = scriptEngine;
        return this;
    }

    public ExecutionStrategy build() {
        if (DDLDefinitionStrategy.INLINE.equals(strategy)) {
            return new ExecutionStrategyInline(dataSource, ddlDefinition, scriptEngine);
        } else if (DDLDefinitionStrategy.FROM_FILE.equals(strategy)) {
            return new ExecutionStrategyFromFile(dataSource, ddlFile);
        } else {
            throw new IllegalStateException("Execution strategy=[%s] not supported.");
        }
    }
}
