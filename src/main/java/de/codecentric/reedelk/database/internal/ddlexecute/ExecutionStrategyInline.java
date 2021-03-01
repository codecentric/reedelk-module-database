package de.codecentric.reedelk.database.internal.ddlexecute;

import de.codecentric.reedelk.database.component.DDLExecute;
import de.codecentric.reedelk.database.internal.exception.DDLExecuteException;
import de.codecentric.reedelk.runtime.api.commons.ComponentPrecondition;
import de.codecentric.reedelk.runtime.api.flow.FlowContext;
import de.codecentric.reedelk.runtime.api.message.Message;
import de.codecentric.reedelk.runtime.api.script.ScriptEngineService;
import de.codecentric.reedelk.runtime.api.script.dynamicvalue.DynamicString;

import javax.sql.DataSource;

import static de.codecentric.reedelk.database.internal.commons.Messages.DDLExecute.DDL_SCRIPT_EVALUATE_ERROR;
import static de.codecentric.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;

class ExecutionStrategyInline extends AbstractExecutionStrategy {

    private final DynamicString ddlDefinition;
    private final ScriptEngineService scriptEngine;

    ExecutionStrategyInline(DataSource dataSource, DynamicString ddlDefinition, ScriptEngineService scriptEngine) {
        super(dataSource);
        ComponentPrecondition.Configuration.requireNotNull(DDLExecute.class, ddlDefinition, "DDL definition string must be defined for DDL execute component.");
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
