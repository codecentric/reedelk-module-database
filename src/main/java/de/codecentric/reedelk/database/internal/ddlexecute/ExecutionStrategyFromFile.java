package de.codecentric.reedelk.database.internal.ddlexecute;

import de.codecentric.reedelk.database.component.DDLExecute;
import de.codecentric.reedelk.runtime.api.commons.ComponentPrecondition;
import de.codecentric.reedelk.runtime.api.commons.StreamUtils;
import de.codecentric.reedelk.runtime.api.flow.FlowContext;
import de.codecentric.reedelk.runtime.api.message.Message;
import de.codecentric.reedelk.runtime.api.resource.ResourceText;

import javax.sql.DataSource;

class ExecutionStrategyFromFile extends AbstractExecutionStrategy {

    private final ResourceText ddlFile;

    ExecutionStrategyFromFile(DataSource dataSource, ResourceText ddlFile) {
        super(dataSource);
        ComponentPrecondition.Configuration.requireNotNull(DDLExecute.class, ddlFile, "DDL file must be defined for DDL execute component.");
        this.ddlFile = ddlFile;
    }

    @Override
    String ddl(FlowContext flowContext, Message message) {
        return StreamUtils.FromString.consume(ddlFile.data());
    }
}
