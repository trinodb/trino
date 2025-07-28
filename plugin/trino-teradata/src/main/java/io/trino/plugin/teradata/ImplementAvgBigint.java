package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.aggregation.BaseImplementAvgBigint;

public class ImplementAvgBigint
        extends BaseImplementAvgBigint
{
    @Override
    protected String getRewriteFormatExpression()
    {
        // Teradata uses FLOAT for double precision floating-point
        // CAST to FLOAT ensures proper decimal division for AVG
        return "avg(CAST(%s AS FLOAT))";
    }
}
