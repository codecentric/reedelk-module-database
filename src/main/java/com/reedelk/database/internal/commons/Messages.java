package com.reedelk.database.internal.commons;

import com.reedelk.runtime.api.commons.FormattedMessage;

public class Messages {

    private Messages() {
    }

    public enum DDLExecute implements FormattedMessage {

        DDL_EXECUTE_ERROR("Could not execute DDL: %s"),
        DDL_EXECUTE_ERROR_WITH_DDL("Could not execute DDL=[%s]: %s"),
        DDL_SCRIPT_EVALUATE_ERROR("Could not evaluate DDL script=[%s]");

        private String message;

        DDLExecute(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Select implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute select query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute select query=[%s]: %s"),
        COLUMN_TYPE_NOT_SUPPORTED("Column type id=[%d] not supported for column name=[%s]"),
        BLOB_TO_BYTES_ERROR("Could not convert bytes from blob, column name=[%s]"),
        CLOB_TO_STRING_ERROR("Could not convert string from clob, column name=[%s]"),
        METADATA_FETCH_ERROR("Could not fetch query metadata, SQL error code=[%d], SQL state=[%s], cause=[%s]");

        private String message;

        Select(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Insert implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute insert query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute insert query=[%s]: %s");

        private String message;

        Insert(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Update implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute update query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute update query=[%s]: %s");

        private String message;

        Update(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }

    public enum Delete implements FormattedMessage {

        QUERY_EXECUTE_ERROR("Could not execute delete query: %s"),
        QUERY_EXECUTE_ERROR_WITH_QUERY("Could not execute delete query=[%s]: %s");

        private String message;

        Delete(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }
}
