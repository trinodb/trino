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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public final class NullPushdownDataTypeTest
{
    private final List<TestCase> testCases = new ArrayList<>();
    private TestCase specialColumn;
    private final boolean connectorExpressionOnly;

    private NullPushdownDataTypeTest(boolean connectorExpressionOnly)
    {
        this.connectorExpressionOnly = connectorExpressionOnly;
    }

    public static NullPushdownDataTypeTest create()
    {
        return new NullPushdownDataTypeTest(false);
    }

    public static NullPushdownDataTypeTest connectorExpressionOnly()
    {
        return new NullPushdownDataTypeTest(true);
    }

    public NullPushdownDataTypeTest addSpecialColumn(String inputType, String inputLiteral, String expectedLiteral)
    {
        checkState(specialColumn == null, "Special column already set");
        checkArgument(!"NULL".equalsIgnoreCase(inputLiteral), "Special column should not be NULL");
        specialColumn = new TestCase(inputType, inputLiteral, Optional.of(expectedLiteral));
        return this;
    }

    public NullPushdownDataTypeTest addTestCase(String inputType)
    {
        testCases.add(new TestCase(inputType, "NULL", Optional.empty()));
        return this;
    }

    public NullPushdownDataTypeTest execute(QueryRunner queryRunner, DataSetup dataSetup)
    {
        return execute(queryRunner, queryRunner.getDefaultSession(), dataSetup);
    }

    public NullPushdownDataTypeTest execute(QueryRunner queryRunner, Session session, DataSetup dataSetup)
    {
        checkState(specialColumn != null, "Null pushdown test requires special column");
        checkState(!testCases.isEmpty(), "No test cases");
        List<ColumnSetup> columns = ImmutableList.<ColumnSetup>builder()
                .add(specialColumn)
                .addAll(testCases)
                .build();
        try (TemporaryRelation temporaryRelation = dataSetup.setupTemporaryRelation(columns)) {
            verifyPredicate(queryRunner, session, temporaryRelation, true, false, !connectorExpressionOnly);
            verifyPredicate(queryRunner, session, temporaryRelation, true, true, true);
            verifyPredicate(queryRunner, session, temporaryRelation, false, false, !connectorExpressionOnly);
            verifyPredicate(queryRunner, session, temporaryRelation, false, true, true);
        }
        return this;
    }

    private void verifyPredicate(QueryRunner queryRunner, Session session, TemporaryRelation temporaryRelation, boolean isNull, boolean connectorExpression, boolean expectPushdown)
    {
        String specialColumnName = "col_0";
        String withConnectorExpression = connectorExpression ? " OR %s IS NULL".formatted(specialColumnName) : "";

        String queryWithAll = "SELECT " + specialColumnName + " FROM " + temporaryRelation.getName() + " WHERE " +
                IntStream.range(0, testCases.size())
                        .mapToObj(column -> getPredicate(column, isNull))
                        .collect(joining(" AND "))
                + withConnectorExpression;

        // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);
        try {
            assertPushdown(expectPushdown,
                    assertResult(isNull ? specialColumn.expectedLiteral() : Optional.empty(),
                            assertThat(queryAssertions.query(session, queryWithAll))));
        }
        catch (AssertionError e) {
            // if failed - identify exact column which caused the failure
            for (int column = 0; column < testCases.size(); column++) {
                String queryWithSingleColumnPredicate = "SELECT " + specialColumnName + " FROM " + temporaryRelation.getName() + " WHERE " + getPredicate(column, isNull) + withConnectorExpression;
                assertPushdown(expectPushdown,
                        assertResult(isNull ? specialColumn.expectedLiteral() : Optional.empty(),
                                assertThat(queryAssertions.query(session, queryWithSingleColumnPredicate))));
            }
            throw new IllegalStateException("Single column assertion should fail for at least one column, if query of all column failed", e);
        }
    }

    private static String getPredicate(int column, boolean isNull)
    {
        String columnName = "col_" + (1 + column);
        return isNull
                ? columnName + " IS NULL"
                : columnName + " IS NOT NULL";
    }

    private static QueryAssert assertResult(Optional<String> value, QueryAssert assertion)
    {
        return value.isPresent()
                ? assertion.matches("VALUES %s".formatted(value.get()))
                : assertion.returnsEmptyResult();
    }

    private static QueryAssert assertPushdown(boolean expectPushdown, QueryAssert assertion)
    {
        return expectPushdown
                ? assertion.isFullyPushedDown()
                : assertion.isNotFullyPushedDown(FilterNode.class);
    }

    private record TestCase(
            String declaredType,
            String inputLiteral,
            Optional<String> expectedLiteral)
            implements ColumnSetup
    {
        private TestCase
        {
            requireNonNull(declaredType, "declaredType is null");
            requireNonNull(inputLiteral, "inputLiteral is null");
            requireNonNull(expectedLiteral, "expectedLiteral is null");
        }

        @Override
        public Optional<String> getDeclaredType()
        {
            return Optional.of(declaredType);
        }

        @Override
        public String getInputLiteral()
        {
            return inputLiteral;
        }
    }
}
