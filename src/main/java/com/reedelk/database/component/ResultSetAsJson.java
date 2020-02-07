package com.reedelk.database.component;

import com.reedelk.database.commons.ResultSetConverter;
import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.ResultRow;
import com.reedelk.runtime.api.message.content.TypedContent;
import com.reedelk.runtime.api.message.content.TypedPublisher;
import org.json.JSONArray;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;

import java.util.List;

@ESBComponent("Result Set As JSON")
@Component(service = ResultSetAsJson.class, scope = ServiceScope.PROTOTYPE)
public class ResultSetAsJson implements ProcessorSync {

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        TypedContent<ResultRow, List<ResultRow>> content = message.content();
        TypedPublisher<ResultRow> resultSet = content.stream();
        try {
            JSONArray convert = ResultSetConverter.convert(resultSet);
            String result = convert.toString(4);
            return MessageBuilder.get().withJson(result).build();
        } catch (Throwable exception) {
            throw new ESBException(exception);
        }
    }
}