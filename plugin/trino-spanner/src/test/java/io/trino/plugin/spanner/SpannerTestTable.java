package io.trino.plugin.spanner;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static java.lang.String.format;

public class SpannerTestTable extends TestTable
{

    public SpannerTestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition)
    {
        super(sqlExecutor, namePrefix, tableDefinition);
    }


    @Override
    public void createAndInsert(List<String> rowsToInsert)
    {
        String[] fields = tableDefinition.split(",");
        String pkField = fields[fields.length - 1].trim();
        String pkColumn = pkField.split(" ")[0];
        String create = "CREATE TABLE %s (%s) PRIMARY KEY (%s)"
                .formatted(
                        this.name,
                        tableDefinition,
                        pkColumn);
        sqlExecutor.execute(create);

        try {
            for (String row : rowsToInsert) {
                String sql = format("INSERT INTO %s VALUES (%s)", name, row);
                sqlExecutor.execute(sql);
            }
        }
        catch (Exception e) {
            try (SpannerTestTable ignored = this) {
                throw e;
            }
        }
    }
}
