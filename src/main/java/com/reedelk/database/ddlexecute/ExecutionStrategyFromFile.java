package com.reedelk.database.ddlexecute;

import com.reedelk.database.component.DDLExecute;
import com.reedelk.runtime.api.commons.StreamUtils;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.resource.ResourceText;

import javax.sql.DataSource;

import static com.reedelk.runtime.api.commons.ConfigurationPreconditions.requireNotNull;

class ExecutionStrategyFromFile extends AbstractExecutionStrategy {

    private final ResourceText ddlFile;

    ExecutionStrategyFromFile(DataSource dataSource, ResourceText ddlFile) {
        super(dataSource);
        requireNotNull(DDLExecute.class, ddlFile, "DDL file must be defined for DDL execute component.");
        this.ddlFile = ddlFile;
    }

    @Override
    String ddl(FlowContext flowContext, Message message) {
        return StreamUtils.FromString.consume(ddlFile.data());
    }
}
