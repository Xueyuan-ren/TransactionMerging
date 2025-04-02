package com.grpc.examples;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.grpc.examples.api.SQLStmt;

public abstract class Procedure {

    private final String procName;

    /**
     * Constructor
     */
    protected Procedure() {
        this.procName = this.getClass().getSimpleName();
    }

    protected final String getProcedureName() {
        return (this.procName);
    }

    public final PreparedStatement getPreparedStatement(Connection conn, SQLStmt stmt) throws SQLException {

        PreparedStatement pStmt = null;
        pStmt = conn.prepareStatement(stmt.getSQL());

        return (pStmt);
    }

    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, int[] is) throws SQLException {

        PreparedStatement pStmt = null;

        // Everyone else can use the regular getGeneratedKeys() method
        if (is != null) {
            pStmt = conn.prepareStatement(stmt.getSQL(), is);
        }
        // They don't care about keys
        else {
            pStmt = conn.prepareStatement(stmt.getSQL());
        }

        return (pStmt);
    }

    public static class UserAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;

        /**
         * Default Constructor
         *
         * @param msg
         * @param ex
         */
        public UserAbortException(String msg, Throwable ex) {
            super(msg, ex);
        }

        /**
         * Constructs a new UserAbortException
         * with the specified detail message.
         */
        public UserAbortException(String msg) {
            this(msg, null);
        }
    }
}
