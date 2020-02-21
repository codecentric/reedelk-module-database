package com.reedelk.database.component;

import com.reedelk.database.commons.Messages;
import com.reedelk.database.commons.ResultSetConverter;
import com.reedelk.runtime.api.annotation.Description;
import com.reedelk.runtime.api.annotation.ModuleComponent;
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

@ModuleComponent("Result Set As JSON")
@Description("Converts a Result Set into a JSON structure.")
@Component(service = ResultSetAsJson.class, scope = ServiceScope.PROTOTYPE)
public class ResultSetAsJson implements ProcessorSync {

    private static final int INDENT_FACTOR = 4;

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        TypedContent<ResultRow, List<ResultRow>> content = message.content();
        TypedPublisher<ResultRow> resultSet = content.stream();

        if (!resultSet.getType().equals(ResultRow.class)) {
            String errorMessage = Messages.ResultSetAsJson.WRONG_ARGUMENT
                    .format(ResultSetAsJson.class.getSimpleName(),
                            ResultRow.class.getSimpleName(),
                            resultSet.getType().getSimpleName());
            throw new IllegalArgumentException(errorMessage);
        }

        try {
            JSONArray convert = ResultSetConverter.convert(resultSet);
            String result = convert.toString(INDENT_FACTOR);

            return MessageBuilder.get()
                    .withJson(result)
                    .build();
        } catch (Throwable exception) {
            throw new ESBException(exception);
        }
    }
}
