package com.reedelk.database.component;

import com.reedelk.database.ResultRow;
import com.reedelk.database.ResultSetConverter;
import com.reedelk.runtime.api.annotation.ESBComponent;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.utils.TypedPublisher;
import org.json.JSONArray;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ServiceScope;
import org.reactivestreams.Publisher;

import java.sql.ResultSet;
import java.sql.SQLException;

@ESBComponent("Result Set As JSON")
@Component(service = ResultSetAsJson.class, scope = ServiceScope.PROTOTYPE)
public class ResultSetAsJson implements ProcessorSync {

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        Object payload = message.payload();
        if (!(payload instanceof TypedPublisher)) {
            throw new IllegalStateException("Result Set Expected");
        }

        TypedPublisher<ResultRow> resultSet = (TypedPublisher<ResultRow>) payload;
        try {
            JSONArray convert = ResultSetConverter.convert(resultSet);
            String result = convert.toString(4);
            return MessageBuilder.get().withJson(result).build();
        } catch (SQLException e) {
            throw new ESBException(e);
        }
    }
}
