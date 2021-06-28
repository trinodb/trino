package io.trino.plugin.ignite;

import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.sql.SqlExecutor;

import java.util.List;

public class TrinoCreateAndInsertDataSetup
        extends CreateAndInsertDataSetup
{
    public TrinoCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        super(sqlExecutor, tableNamePrefix);
    }

    @Override
    protected String tableDefinition(List<ColumnSetup> inputs)
    {
        return super.tableDefinition(inputs) + "WITH (\n primary_key = ARRAY['col_0'] \n)";
    }
}
