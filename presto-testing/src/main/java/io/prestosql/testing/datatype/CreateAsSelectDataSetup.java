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
package io.prestosql.testing.datatype;

import com.google.common.base.Joiner;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;

import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public class CreateAsSelectDataSetup
        implements DataSetup
{
    private final SqlExecutor sqlExecutor;
    private final String tableNamePrefix;

    public CreateAsSelectDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        this.sqlExecutor = sqlExecutor;
        this.tableNamePrefix = tableNamePrefix;
    }

    @Override
    public TestTable setupTestTable(List<ColumnSetup> inputs)
    {
        List<String> columnValues = inputs.stream()
                .map(this::literalInExplicitCast)
                .collect(toList());
        Stream<String> columnValuesWithNames = range(0, columnValues.size())
                .mapToObj(i -> format("%s col_%d", columnValues.get(i), i));
        String selectBody = Joiner.on(",\n").join(columnValuesWithNames.iterator());
        return new TestTable(sqlExecutor, tableNamePrefix, "AS SELECT " + selectBody);
    }

    private String literalInExplicitCast(ColumnSetup input)
    {
        return format(
                "CAST(%s AS %s)",
                input.getInputLiteral(),
                input.getDeclaredType().orElseThrow(() -> new IllegalArgumentException("declared type not set")));
    }
}
