/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.ydb;

import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TemporaryRelation;
import io.trino.testing.sql.TestTable;

import java.util.List;
import java.util.stream.IntStream;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class YdbCreateAndInsertDataSetup
        implements DataSetup
{
    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;

    public YdbCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.tableNamePrefix = requireNonNull(tableNamePrefix, "tableNamePrefix is null");
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        // YDB tables require a primary key, so the first column is the key
        String definition = IntStream.range(0, inputs.size())
                .mapToObj(column -> "col_%d %s".formatted(column, inputs.get(column).getDeclaredType().orElseThrow()))
                .collect(joining(", ", "(", ", PRIMARY KEY (col_0))"));
        TestTable table = new TestTable(sqlExecutor, tableNamePrefix, definition);
        try {
            String columns = IntStream.range(0, inputs.size())
                    .mapToObj(column -> "col_" + column)
                    .collect(joining(", "));
            String values = inputs.stream()
                    .map(ColumnSetup::getInputLiteral)
                    .collect(joining(", "));
            sqlExecutor.execute("INSERT INTO %s (%s) VALUES (%s)".formatted(table.getName(), columns, values));
        }
        catch (RuntimeException e) {
            closeAllSuppress(e, table);
            throw e;
        }
        return table;
    }
}
