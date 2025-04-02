package com.grpc.examples.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SQLStmt {
    private static final Pattern SUBSTITUTION_PATTERN = Pattern.compile("\\?\\?");

    private String orig_sql;
    private String sql;

    /**
     * For each unique '??' that we encounter in the SQL for this Statement,
     * we will substitute it with the number of '?' specified in this array.
     */
    private final int[] substitutions;

    /**
     * Constructor
     *
     * @param sql
     * @param substitutions
     */
    public SQLStmt(String sql, int... substitutions) {
        this.substitutions = substitutions;
        this.setSQL(sql);
    }

    /**
     * Magic SQL setter!
     * Each occurrence of the pattern "??" will be replaced by a string
     * of repeated ?'s
     *
     * @param sql
     */
    public final void setSQL(String sql) {
        this.orig_sql = sql.trim();
        for (int ctr : this.substitutions) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < ctr; i++) {
                sb.append(i > 0 ? ", " : "").append("?");
            }
            Matcher m = SUBSTITUTION_PATTERN.matcher(sql);
            String replace = sb.toString();
            sql = m.replaceFirst(replace);
        }
        this.sql = sql;
        // if (LOG.isDebugEnabled()) {
        //     LOG.debug("Initialized SQL:\n{}", this.sql);
        // }
    }

    public final String getSQL() {
        return (this.sql);
    }

    protected final String getOriginalSQL() {
        return (this.orig_sql);
    }

    @Override
    public String toString() {
        return "SQLStmt{" + this.sql + "}";
    }
}
