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
