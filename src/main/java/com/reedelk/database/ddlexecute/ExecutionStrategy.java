package com.reedelk.database.ddlexecute;

import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;

public interface ExecutionStrategy {

    Message execute(FlowContext flowContext, Message message);

}
