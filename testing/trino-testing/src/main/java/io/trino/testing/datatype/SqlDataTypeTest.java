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

import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public final class SqlDataTypeTest
{
    public static SqlDataTypeTest create()
    {
        return new SqlDataTypeTest();
    }

    private final List<TestCase> testCases = new ArrayList<>();

    private SqlDataTypeTest() {}

    public SqlDataTypeTest addRoundTrip(String literal)
    {
        return addRoundTrip(literal, literal);
    }

    public SqlDataTypeTest addRoundTrip(String inputLiteral, String expectedLiteral)
    {
        testCases.add(new TestCase(Optional.empty(), inputLiteral, Optional.empty(), expectedLiteral));
        return this;
    }

    public SqlDataTypeTest addRoundTrip(String inputType, String literal, Type expectedType)
    {
        return addRoundTrip(inputType, literal, expectedType, literal);
    }

    public SqlDataTypeTest addRoundTrip(String inputType, String inputLiteral, Type expectedType, String expectedLiteral)
    {
        testCases.add(new TestCase(Optional.of(inputType), inputLiteral, Optional.of(expectedType), expectedLiteral));
        return this;
    }

    public SqlDataTypeTest execute(QueryRunner queryRunner, DataSetup dataSetup)
    {
        return execute(queryRunner, queryRunner.getDefaultSession(), dataSetup);
    }

    public SqlDataTypeTest execute(QueryRunner queryRunner, Session session, DataSetup dataSetup)
    {
        checkState(!testCases.isEmpty(), "No test cases");
        try (TestTable testTable = dataSetup.setupTestTable(unmodifiableList(testCases))) {
            verifySelect(queryRunner, session, testTable);
            verifyPredicate(queryRunner, session, testTable);
        }
        return this;
    }

    private void verifySelect(QueryRunner queryRunner, Session session, TestTable testTable)
    {
        @SuppressWarnings("resource") // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);

        QueryAssert assertion = assertThat(queryAssertions.query(session, "SELECT * FROM " + testTable.getName()));
        MaterializedResult expected = queryRunner.execute(session, testCases.stream()
                .map(TestCase::getExpectedLiteral)
                .collect(joining(",", "VALUES ROW(", ")")));

        // Verify types if specified
        for (int column = 0; column < testCases.size(); column++) {
            TestCase testCase = testCases.get(column);
            if (testCase.getExpectedType().isPresent()) {
                Type expectedType = testCase.getExpectedType().get();
                assertion.outputHasType(column, expectedType);
                assertThat(expected.getTypes())
                        .as(format("Expected literal type at column %d (check consistency of expected type and expected literal)", column + 1))
                        .element(column).isEqualTo(expectedType);
            }
        }

        assertion.matches(expected);
    }

    private void verifyPredicate(QueryRunner queryRunner, Session session, TestTable testTable)
    {
        String queryWithAll = "SELECT 'all found' FROM " + testTable.getName() + " WHERE " +
                IntStream.range(0, testCases.size())
                        .mapToObj(this::getPredicate)
                        .collect(joining(" AND "));

        MaterializedResult result = queryRunner.execute(session, queryWithAll);
        if (result.getOnlyColumnAsSet().equals(Set.of("all found"))) {
            return;
        }

        @SuppressWarnings("resource") // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);

        for (int column = 0; column < testCases.size(); column++) {
            assertThat(queryAssertions.query(session, "SELECT 'found' FROM " + testTable.getName() + " WHERE " + getPredicate(column)))
                    .matches("VALUES 'found'");
        }
    }

    private String getPredicate(int column)
    {
        return format("col_%s IS NOT DISTINCT FROM %s", column, testCases.get(column).getExpectedLiteral());
    }

    private static class TestCase
            implements ColumnSetup
    {
        private final Optional<String> declaredType;
        private final String inputLiteral;
        private final Optional<Type> expectedType;
        private final String expectedLiteral;

        public TestCase(Optional<String> declaredType, String inputLiteral, Optional<Type> expectedType, String expectedLiteral)
        {
            this.declaredType = requireNonNull(declaredType, "declaredType is null");
            this.expectedType = requireNonNull(expectedType, "expectedType is null");
            this.inputLiteral = requireNonNull(inputLiteral, "inputLiteral is null");
            this.expectedLiteral = requireNonNull(expectedLiteral, "expectedLiteral is null");
        }

        @Override
        public Optional<String> getDeclaredType()
        {
            return declaredType;
        }

        @Override
        public String getInputLiteral()
        {
            return inputLiteral;
        }

        public Optional<Type> getExpectedType()
        {
            return expectedType;
        }

        public String getExpectedLiteral()
        {
            return expectedLiteral;
        }
    }
}
