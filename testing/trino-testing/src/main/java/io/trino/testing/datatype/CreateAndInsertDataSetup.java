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
package io.trino.testing.datatype;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class CreateAndInsertDataSetup
        implements DataSetup
{
    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;

    public CreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        this.sqlExecutor = sqlExecutor;
        this.tableNamePrefix = tableNamePrefix;
    }

    @Override
    public TestTable setupTestTable(List<ColumnSetup> inputs)
    {
        TestTable testTable = createTestTable(inputs);
        try {
            insertRows(testTable, inputs);
        }
        catch (Exception e) {
            closeQuietly(testTable);
            throw e;
        }
        return testTable;
    }

    // TODO replace with closeAllSuppress when https://github.com/airlift/airlift/pull/889 is available
    private static void closeQuietly(AutoCloseable autoCloseable)
    {
        //noinspection EmptyTryBlock
        try (AutoCloseable ignore = autoCloseable) {
            // suppress empty try-catch warning
        }
        catch (Exception ignored) {
        }
    }

    private void insertRows(TestTable testTable, List<ColumnSetup> inputs)
    {
        String valueLiterals = inputs.stream()
                .map(ColumnSetup::getInputLiteral)
                .collect(joining(", "));
        sqlExecutor.execute(format("INSERT INTO %s VALUES(%s)", testTable.getName(), valueLiterals));
    }

    private TestTable createTestTable(List<ColumnSetup> inputs)
    {
        return new TestTable(sqlExecutor, tableNamePrefix, tableDefinition(inputs));
    }

    protected String tableDefinition(List<ColumnSetup> inputs)
    {
        if (inputs.stream().allMatch(input -> input.getDeclaredType().isPresent())) {
            // When all types are explicitly specified, use ordinary CREATE TABLE
            return IntStream.range(0, inputs.size())
                    .mapToObj(column -> format("col_%d %s", column, inputs.get(column).getDeclaredType().orElseThrow()))
                    .collect(joining(",\n", "(\n", ")"));
        }

        return IntStream.range(0, inputs.size())
                .mapToObj(column -> {
                    ColumnSetup input = inputs.get(column);
                    if (input.getDeclaredType().isEmpty()) {
                        return format("%s AS col_%d", input.getInputLiteral(), column);
                    }

                    return format("CAST(%s AS %s) AS col_%d", input.getInputLiteral(), input.getDeclaredType().get(), column);
                })
                .collect(joining(",\n", "AS\nSELECT\n", "\nWHERE 'with no' = 'data'"));
    }
}
