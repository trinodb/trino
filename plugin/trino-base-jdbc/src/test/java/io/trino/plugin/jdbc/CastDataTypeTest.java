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

import com.google.common.collect.ImmutableList;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class CastDataTypeTest
{
    private final List<TestCaseInput> testCaseInputs = new ArrayList<>();

    private OptionalInt inputSize = OptionalInt.empty();
    private TestTable testTable;

    private CastDataTypeTest() {}

    public static CastDataTypeTest create()
    {
        return new CastDataTypeTest();
    }

    public CastDataTypeTest addColumn(String columnName, String columnType, List<Object> inputValues)
    {
        if (inputSize.isEmpty()) {
            inputSize = OptionalInt.of(inputValues.size());
        }
        checkArgument(inputSize.getAsInt() == inputValues.size(), "Expected input size: %s, but found: %s", inputSize.getAsInt(), inputValues.size());
        testCaseInputs.add(new TestCaseInput(columnName, columnType, inputValues));
        return this;
    }

    public CastDataTypeTest execute(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        checkState(!testCaseInputs.isEmpty(), "No test case inputs");
        List<String> columnsWithType = new ArrayList<>();
        for (TestCaseInput input : testCaseInputs) {
            columnsWithType.add("%s %s".formatted(input.columnName(), input.columnType()));
        }
        String tableDefinition = columnsWithType.stream()
                .collect(joining(", ", "(", ")"));

        ImmutableList.Builder<String> rowToInsert = ImmutableList.builder();
        if (inputSize.isPresent()) {
            for (int i = 0; i < inputSize.getAsInt(); i++) {
                StringJoiner rowBuilder = new StringJoiner(",");
                for (TestCaseInput input : testCaseInputs) {
                    rowBuilder.add(String.valueOf(input.inputValues().get(i)));
                }
                rowToInsert.add(rowBuilder.toString());
            }
        }
        testTable = new TestTable(sqlExecutor, tableNamePrefix, tableDefinition, rowToInsert.build());
        return this;
    }

    public String tableName()
    {
        return testTable.getName();
    }

    public void close()
    {
        testTable.close();
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

    public record CastTestCase(String sourceColumn, String castType, Optional<String> targetColumn)
    {
        public CastTestCase(String sourceColumn, String castType)
        {
            this(sourceColumn, castType, Optional.empty());
        }

        public CastTestCase
        {
            requireNonNull(sourceColumn, "sourceColumn is null");
            requireNonNull(castType, "castType is null");
            requireNonNull(targetColumn, "targetColumn is null");
        }
    }
}
