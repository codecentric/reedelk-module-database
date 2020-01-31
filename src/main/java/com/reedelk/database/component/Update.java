package com.reedelk.database.component;

import com.reedelk.database.ConnectionConfiguration;
import com.reedelk.database.ConnectionPools;
import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

@ESBComponent("SQL Update")
@Component(service = Update.class, scope = ServiceScope.PROTOTYPE)
public class Update implements ProcessorSync {

    @Property("Connection Configuration")
    private ConnectionConfiguration connectionConfiguration;
    @Property("SQL Query")
    private String query;

    @Reference
    private ConnectionPools pools;

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        return null;
    }
}
