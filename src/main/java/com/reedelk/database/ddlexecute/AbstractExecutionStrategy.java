package com.reedelk.database.ddlexecute;

import com.reedelk.database.commons.DatabaseUtils;
import com.reedelk.runtime.api.exception.ESBException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

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

            // TODO: The message should contain in the attributes the executed DDL.
            return MessageBuilder.get().withJavaObject(rowCount).build();

        } catch (Throwable exception) {
            // TODO: Log this exception. If ddl to execute != null then log it!
            throw new ESBException(exception);
        } finally {
            DatabaseUtils.closeSilently(resultSet);
            DatabaseUtils.closeSilently(statement);
            DatabaseUtils.closeSilently(connection);
        }
    }

    abstract String ddl(FlowContext flowContext, Message message);
}
