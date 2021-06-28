package io.trino.plugin.ignite;

import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.sql.SqlExecutor;

import java.util.List;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class IgniteCreateAndInsertDataSetup
        extends CreateAndInsertDataSetup
{
    public IgniteCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        super(sqlExecutor, tableNamePrefix);
    }

    @Override
    protected String tableDefinition(List<ColumnSetup> inputs)
    {
        return format("(col_0 %s primary key", inputs.get(0).getDeclaredType().orElseThrow())
                + IntStream.range(1, inputs.size())
                .mapToObj(column -> format("col_%d %s", column, inputs.get(column).getDeclaredType().orElseThrow()))
                .collect(joining(",\n", ",", ")"));
    }
}
