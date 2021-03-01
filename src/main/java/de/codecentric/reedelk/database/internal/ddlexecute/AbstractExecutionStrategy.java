package de.codecentric.reedelk.database.internal.ddlexecute;

import de.codecentric.reedelk.database.component.DDLExecute;
import de.codecentric.reedelk.database.internal.attribute.DDLExecuteAttributes;
import de.codecentric.reedelk.database.internal.commons.DatabaseUtils;
import de.codecentric.reedelk.database.internal.exception.DDLExecuteException;
import de.codecentric.reedelk.runtime.api.flow.FlowContext;
import de.codecentric.reedelk.runtime.api.message.Message;
import de.codecentric.reedelk.runtime.api.message.MessageAttributes;
import de.codecentric.reedelk.runtime.api.message.MessageBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;

import static de.codecentric.reedelk.database.internal.commons.Messages.DDLExecute.DDL_EXECUTE_ERROR;
import static de.codecentric.reedelk.database.internal.commons.Messages.DDLExecute.DDL_EXECUTE_ERROR_WITH_DDL;
import static de.codecentric.reedelk.runtime.api.commons.StackTraceUtils.rootCauseMessageOf;

abstract class AbstractExecutionStrategy implements ExecutionStrategy {

    private final DataSource dataSource;

    AbstractExecutionStrategy(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Message execute(FlowContext flowContext, Message message) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        String ddlToExecute = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();

            ddlToExecute = ddl(flowContext, message);

            int rowCount = statement.executeUpdate(ddlToExecute);

            MessageAttributes attributes = new DDLExecuteAttributes(ddlToExecute);

            return MessageBuilder.get(DDLExecute.class)
                    .withJavaObject(rowCount)
                    .attributes(attributes)
                    .build();

        } catch (Throwable exception) {
            String error = Optional.ofNullable(ddlToExecute)
                    .map(ddl -> DDL_EXECUTE_ERROR_WITH_DDL.format(ddl, rootCauseMessageOf(exception)))
                    .orElse(DDL_EXECUTE_ERROR.format(rootCauseMessageOf(exception)));
            throw new DDLExecuteException(error, exception);

        } finally {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);
        }
    }

    abstract String ddl(FlowContext flowContext, Message message);
}
