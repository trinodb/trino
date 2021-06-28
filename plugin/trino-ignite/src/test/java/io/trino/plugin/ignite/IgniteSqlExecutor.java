package io.trino.plugin.ignite;

import io.trino.testing.sql.SqlExecutor;

import static java.util.Objects.requireNonNull;

public class IgniteSqlExecutor
        implements SqlExecutor
{
    private final SqlExecutor delegate;

    public IgniteSqlExecutor(SqlExecutor delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void execute(String sql)
    {
        if (sql.startsWith("CREATE TABLE")) {
            sql = sql + " WITH (\n primary_key = ARRAY['col_0'] \n)";
        }
        delegate.execute(sql);
    }
}
