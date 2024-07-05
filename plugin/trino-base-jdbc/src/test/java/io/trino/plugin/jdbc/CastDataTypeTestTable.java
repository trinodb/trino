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
package io.trino.plugin.jdbc;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TemporaryRelation;
import io.trino.testing.sql.TestTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeAll;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class CastDataTypeTestTable
        implements TemporaryRelation
{
    private final List<TestCaseInput> testCaseRows = new ArrayList<>();
    private final int rowCount;

    private TestTable testTable;

    private CastDataTypeTestTable(int rowCount)
    {
        this.rowCount = rowCount;
    }

    public static CastDataTypeTestTable create(int rowCount)
    {
        return new CastDataTypeTestTable(rowCount);
    }

    public CastDataTypeTestTable addColumn(String columnName, String columnType, List<Object> inputValues)
    {
        checkArgument(rowCount == inputValues.size(), "Expected input size: %s, but found: %s", rowCount, inputValues.size());
        testCaseRows.add(new TestCaseInput(columnName, columnType, inputValues));
        return this;
    }

    public CastDataTypeTestTable execute(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        checkState(!testCaseRows.isEmpty(), "No test case rows");
        List<String> columnsWithType = new ArrayList<>();
        for (TestCaseInput input : testCaseRows) {
            columnsWithType.add("%s %s".formatted(input.columnName(), input.columnType()));
        }
        String tableDefinition = columnsWithType.stream()
                .collect(joining(", ", "(", ")"));

        List<String> rowsToInsert = IntStream.range(0, rowCount)
                .mapToObj(rowId -> testCaseRows.stream()
                        .map(TestCaseInput::inputValues)
                        .map(rows -> rows.get(rowId))
                        .map(String::valueOf)
                        .collect(joining(",")))
                .collect(toImmutableList());
        testTable = new TestTable(sqlExecutor, tableNamePrefix, tableDefinition, rowsToInsert);
        return this;
    }

    @Override
    public String getName()
    {
        return testTable.getName();
    }

    @Override
    public void close()
    {
        try {
            closeAll(testTable);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public record TestCaseInput(String columnName, String columnType, List<Object> inputValues)
    {
        public TestCaseInput
        {
            requireNonNull(columnName, "columnName is null");
            requireNonNull(columnType, "columnType is null");
            inputValues = new ArrayList<>(requireNonNull(inputValues, "inputValues is null"));
        }
    }
}
