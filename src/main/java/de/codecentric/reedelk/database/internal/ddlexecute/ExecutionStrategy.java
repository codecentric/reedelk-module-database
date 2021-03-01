package de.codecentric.reedelk.database.internal.ddlexecute;

import de.codecentric.reedelk.runtime.api.flow.FlowContext;
import de.codecentric.reedelk.runtime.api.message.Message;

public interface ExecutionStrategy {

    Message execute(FlowContext flowContext, Message message);

}
