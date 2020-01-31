package com.reedelk.mysql.component;

import com.reedelk.mysql.ConnectionConfiguration;
import com.reedelk.mysql.ConnectionPools;
import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.annotation.Property;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ServiceScope;

@ESBComponent("SQL Delete")
@Component(service = Delete.class, scope = ServiceScope.PROTOTYPE)
public class Delete implements ProcessorSync {

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
