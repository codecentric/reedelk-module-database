package com.reedelk.database.commons;

import com.reedelk.runtime.api.message.content.ResultRow;

public class Messages {

    private Messages() {
    }

    private static String formatMessage(String template, Object ...args) {
        return String.format(template, args);
    }

    interface FormattedMessage {
        String format(Object ...args);
    }

    public enum DDLExecute implements FormattedMessage {

        DDL_EXECUTE_ERROR("Could not execute DDL: %s"),
        DDL_EXECUTE_ERROR_WITH_DDL("Could not execute DDL=[%s]: %s"),
        DDL_SCRIPT_EVALUATE_ERROR("Could not evaluate DDL script=[%s]");

        private String msg;

        DDLExecute(String msg) {
            this.msg = msg;
        }

        @Override
        public String format(Object... args) {
            return formatMessage(msg, args);
        }
    }

    public enum Select implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute select query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute select query=[%s]: %s");

        private String msg;

        Select(String msg) {
            this.msg = msg;
        }

        @Override
        public String format(Object... args) {
            return formatMessage(msg, args);
        }
    }

    public enum Insert implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute insert query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute insert query=[%s]: %s");

        private String msg;

        Insert(String msg) {
            this.msg = msg;
        }

        @Override
        public String format(Object... args) {
            return formatMessage(msg, args);
        }
    }

    public enum Update implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute update query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute update query=[%s]: %s");

        private String msg;

        Update(String msg) {
            this.msg = msg;
        }

        @Override
        public String format(Object... args) {
            return formatMessage(msg, args);
        }
    }

    public enum Delete implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute delete query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute delete query=[%s]: %s");

        private String msg;

        Delete(String msg) {
            this.msg = msg;
        }

        @Override
        public String format(Object... args) {
            return formatMessage(msg, args);
        }
    }

    public enum ResultSetAsJson implements FormattedMessage {

        WRONG_ARGUMENT("%s component expects message with payload of type=[%s] " +
                "but type=[%s] was given.");

        private String msg;

        ResultSetAsJson(String msg) {
            this.msg = msg;
        }

        @Override
        public String format(Object... args) {
            return formatMessage(msg, args);
        }
    }
}
