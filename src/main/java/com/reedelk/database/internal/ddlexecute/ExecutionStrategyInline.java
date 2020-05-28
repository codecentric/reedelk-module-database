package com.reedelk.database.internal.ddlexecute;

import com.reedelk.database.component.DDLExecute;
import com.reedelk.database.internal.exception.DDLExecuteException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.script.ScriptEngineService;
import com.reedelk.runtime.api.script.dynamicvalue.DynamicString;

import javax.sql.DataSource;

import static com.reedelk.database.internal.commons.Messages.DDLExecute.DDL_SCRIPT_EVALUATE_ERROR;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotNull;

class ExecutionStrategyInline extends AbstractExecutionStrategy {

    private final DynamicString ddlDefinition;
    private final ScriptEngineService scriptEngine;

    ExecutionStrategyInline(DataSource dataSource, DynamicString ddlDefinition, ScriptEngineService scriptEngine) {
        super(dataSource);
        requireNotNull(DDLExecute.class, ddlDefinition, "DDL definition string must be defined for DDL execute component.");
        requireNotBlank(DDLExecute.class, ddlDefinition.value(), "DDL definition string must not empty for DDL execute component.");
        this.ddlDefinition = ddlDefinition;
        this.scriptEngine = scriptEngine;
    }

    @Override
    String ddl(FlowContext flowContext, Message message) {
        return scriptEngine.evaluate(ddlDefinition, flowContext, message)
                .orElseThrow(() -> new DDLExecuteException(DDL_SCRIPT_EVALUATE_ERROR.format(ddlDefinition.value())));
    }
}
