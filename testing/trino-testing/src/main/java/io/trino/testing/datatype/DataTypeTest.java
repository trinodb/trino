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

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class DataTypeTest
{
    private static final Logger log = Logger.get(DataTypeTest.class);

    private final List<Input<?>> inputs = new ArrayList<>();

    private boolean runSelectWithWhere;

    private DataTypeTest(boolean runSelectWithWhere)
    {
        this.runSelectWithWhere = runSelectWithWhere;
    }

    /**
     * @deprecated Use {@link SqlDataTypeTest#create()} imstead. You can find
     * {@link DataTypeTestToSqlDataTypeTestConverter#create()} helpful for converting the code.
     */
    @Deprecated
    public static DataTypeTest create()
    {
        return new DataTypeTest(false);
    }

    /**
     * @deprecated Use {@link SqlDataTypeTest#create()} imstead. You can find
     * {@link DataTypeTestToSqlDataTypeTestConverter#create()} helpful for converting the code.
     */
    @Deprecated
    public static DataTypeTest create(boolean runSelectWithWhere)
    {
        return new DataTypeTest(runSelectWithWhere);
    }

    public <T> DataTypeTest addRoundTrip(DataType<T> dataType, T value)
    {
        return addRoundTrip(dataType, value, true);
    }

    public <T> DataTypeTest addRoundTrip(DataType<T> dataType, T value, boolean useInWhereClause)
    {
        inputs.add(new Input<>(dataType, value, useInWhereClause));
        return this;
    }

    public DataTypeTest execute(QueryRunner trinoExecutor, DataSetup dataSetup)
    {
        return execute(trinoExecutor, trinoExecutor.getDefaultSession(), dataSetup);
    }

    public DataTypeTest execute(QueryRunner trinoExecutor, Session session, DataSetup dataSetup)
    {
        List<Type> expectedTypes = inputs.stream().map(Input::getTrinoResultType).collect(toList());
        List<Object> expectedResults = inputs.stream().map(Input::toTrinoQueryResult).collect(toList());
        try (TestTable testTable = dataSetup.setupTestTable(unmodifiableList(inputs))) {
            MaterializedResult materializedRows = trinoExecutor.execute(session, "SELECT * from " + testTable.getName());
            checkResults(expectedTypes, expectedResults, materializedRows);
            if (runSelectWithWhere) {
                queryWithWhere(trinoExecutor, session, expectedTypes, expectedResults, testTable);
            }
        }
        return this;
    }

    private void queryWithWhere(QueryRunner trinoExecutor, Session session, List<Type> expectedTypes, List<Object> expectedResults, TestTable testTable)
    {
        String trinoQuery = buildTrinoQueryWithWhereClauses(testTable);
        try {
            MaterializedResult filteredRows = trinoExecutor.execute(session, trinoQuery);
            checkResults(expectedTypes, expectedResults, filteredRows);
        }
        catch (RuntimeException e) {
            log.error(e, "Exception caught during query with merged WHERE clause, querying one column at a time");
            debugTypes(trinoExecutor, session, expectedTypes, expectedResults, testTable);
        }
    }

    private void debugTypes(QueryRunner trinoExecutor, Session session, List<Type> expectedTypes, List<Object> expectedResults, TestTable testTable)
    {
        for (int i = 0; i < inputs.size(); i++) {
            Input<?> input = inputs.get(i);
            if (input.isUseInWhereClause()) {
                String debugQuery = format("SELECT col_%d FROM %s WHERE col_%d IS NOT DISTINCT FROM %s", i, testTable.getName(), i, input.toTrinoLiteral());
                log.info("Querying input: %d (expected type: %s, expectedResult: %s) using: %s", i, expectedTypes.get(i), expectedResults.get(i), debugQuery);
                MaterializedResult debugRows = trinoExecutor.execute(session, debugQuery);
                checkResults(expectedTypes.subList(i, i + 1), expectedResults.subList(i, i + 1), debugRows);
            }
        }
    }

    private String buildTrinoQueryWithWhereClauses(TestTable testTable)
    {
        List<String> predicates = new ArrayList<>();
        for (int i = 0; i < inputs.size(); i++) {
            Input<?> input = inputs.get(i);
            if (input.isUseInWhereClause()) {
                predicates.add(format("col_%d IS NOT DISTINCT FROM %s", i, input.toTrinoLiteral()));
            }
        }
        return "SELECT * FROM " + testTable.getName() + " WHERE " + join(" AND ", predicates);
    }

    private void checkResults(List<Type> expectedTypes, List<Object> expectedResults, MaterializedResult materializedRows)
    {
        assertThat(materializedRows.getTypes()).isEqualTo(expectedTypes);
        List<Object> actualResults = getOnlyElement(materializedRows).getFields();
        verify(actualResults.size() == expectedResults.size(), "lists don't have the same size");
        for (int i = 0; i < expectedResults.size(); i++) {
            assertEquals(actualResults.get(i), expectedResults.get(i), "Element " + i);
        }
    }

    public static class Input<T>
            implements ColumnSetup
    {
        private final DataType<T> dataType;
        private final T value;
        private final boolean useInWhereClause;

        public Input(DataType<T> dataType, T value, boolean useInWhereClause)
        {
            this.dataType = dataType;
            this.value = value;
            this.useInWhereClause = useInWhereClause;
        }

        public boolean isUseInWhereClause()
        {
            return useInWhereClause;
        }

        @Override
        public Optional<String> getDeclaredType()
        {
            return Optional.of(getInsertType());
        }

        public String getInsertType()
        {
            return dataType.getInsertType();
        }

        Type getTrinoResultType()
        {
            return dataType.getTrinoResultType();
        }

        Object toTrinoQueryResult()
        {
            return dataType.toTrinoQueryResult(value);
        }

        @Override
        public String getInputLiteral()
        {
            return toLiteral();
        }

        public String toLiteral()
        {
            return dataType.toLiteral(value);
        }

        public String toTrinoLiteral()
        {
            return dataType.toTrinoLiteral(value);
        }
    }
}
