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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class TestPostgreSqlClient
{
    private static final JdbcColumnHandle BIGINT_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_bigint")
                    .setColumnType(BIGINT)
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.BIGINT, Optional.of("int8"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                    .build();

    private static final JdbcColumnHandle DOUBLE_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_double")
                    .setColumnType(DOUBLE)
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()))
                    .build();

    private static final JdbcColumnHandle VARCHAR_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_varchar")
                    .setColumnType(createVarcharType(10))
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), Optional.of(10), Optional.empty(), Optional.empty(), Optional.empty()))
                    .build();

    private static final JdbcClient JDBC_CLIENT = new PostgreSqlClient(
            new BaseJdbcConfig(),
            new PostgreSqlConfig(),
            new JdbcStatisticsConfig(),
            session -> { throw new UnsupportedOperationException(); },
            new DefaultQueryBuilder(),
            TESTING_TYPE_MANAGER,
            new DefaultIdentifierMapping());

    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(PLANNER_CONTEXT);

    @Test
    public void testImplementCount()
    {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        Variable doubleVariable = new Variable("v_double", BIGINT);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // count(*)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty()),
                Map.of(),
                Optional.of("count(*)"));

        // count(bigint)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("count(\"c_bigint\")"));

        // count(double)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("count(\"c_double\")"));

        // count(DISTINCT bigint)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("count(DISTINCT \"c_bigint\")"));

        // count() FILTER (WHERE ...)

        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, filter),
                Map.of(),
                Optional.empty());

        // count(bigint) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), false, filter),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty());
    }

    @Test
    public void testImplementSum()
    {
        Variable bigintVariable = new Variable("v_bigint", BIGINT);
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // sum(bigint)
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("sum(\"c_bigint\")"));

        // sum(double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(\"c_double\")"));

        // sum(DISTINCT bigint)
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty());  // distinct not supported

        // sum(bigint) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), false, filter),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty()); // filter not supported
    }

    private static void testImplementAggregation(AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments, Optional<String> expectedExpression)
    {
        Optional<JdbcExpression> result = JDBC_CLIENT.implementAggregation(SESSION, aggregateFunction, assignments);
        if (expectedExpression.isEmpty()) {
            assertThat(result).isEmpty();
        }
        else {
            assertThat(result).isPresent();
            assertEquals(result.get().getExpression(), expectedExpression.get());
            Optional<ColumnMapping> columnMapping = JDBC_CLIENT.toColumnMapping(SESSION, null, result.get().getJdbcTypeHandle());
            assertTrue(columnMapping.isPresent(), "No mapping for: " + result.get().getJdbcTypeHandle());
            assertEquals(columnMapping.get().getType(), aggregateFunction.getOutputType());
        }
    }

    @Test
    public void testConvertOr()
    {
        assertThat(JDBC_CLIENT.convertPredicate(
                SESSION,
                translateToConnectorExpression(
                        new LogicalExpression(
                                LogicalExpression.Operator.OR,
                                List.of(
                                        new ComparisonExpression(
                                                ComparisonExpression.Operator.EQUAL,
                                                new SymbolReference("c_bigint_symbol"),
                                                LITERAL_ENCODER.toExpression(TEST_SESSION, 42L, BIGINT)),
                                        new ComparisonExpression(
                                                ComparisonExpression.Operator.EQUAL,
                                                new SymbolReference("c_bigint_symbol_2"),
                                                LITERAL_ENCODER.toExpression(TEST_SESSION, 415L, BIGINT)))),
                        Map.of(
                                "c_bigint_symbol", BIGINT,
                                "c_bigint_symbol_2", BIGINT)),
                Map.of(
                        "c_bigint_symbol", BIGINT_COLUMN,
                        "c_bigint_symbol_2", BIGINT_COLUMN)))
                .hasValue("((\"c_bigint\") = (42)) OR ((\"c_bigint\") = (415))");
    }

    @Test(dataProvider = "testConvertComparisonDataProvider")
    public void testConvertComparison(ComparisonExpression.Operator operator)
    {
        Optional<String> converted = JDBC_CLIENT.convertPredicate(
                SESSION,
                translateToConnectorExpression(
                        new ComparisonExpression(
                                operator,
                                new SymbolReference("c_bigint_symbol"),
                                LITERAL_ENCODER.toExpression(TEST_SESSION, 42L, BIGINT)),
                        Map.of("c_bigint_symbol", BIGINT)),
                Map.of("c_bigint_symbol", BIGINT_COLUMN));

        switch (operator) {
            case EQUAL:
            case NOT_EQUAL:
                assertThat(converted).hasValue(format("(\"c_bigint\") %s (42)", operator.getValue()));
                return;
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IS_DISTINCT_FROM:
                // Not supported yet, even for bigint
                assertThat(converted).isEmpty();
                return;
        }
        throw new UnsupportedOperationException("Unsupported operator: " + operator);
    }

    @DataProvider
    public static Object[][] testConvertComparisonDataProvider()
    {
        return Stream.of(ComparisonExpression.Operator.values())
                .collect(toDataProvider());
    }

    @Test(dataProvider = "testConvertArithmeticBinaryDataProvider")
    public void testConvertArithmeticBinary(ArithmeticBinaryExpression.Operator operator)
    {
        Optional<String> converted = JDBC_CLIENT.convertPredicate(
                SESSION,
                translateToConnectorExpression(
                        new ArithmeticBinaryExpression(
                                operator,
                                new SymbolReference("c_bigint_symbol"),
                                LITERAL_ENCODER.toExpression(TEST_SESSION, 42L, BIGINT)),
                        Map.of("c_bigint_symbol", BIGINT)),
                Map.of("c_bigint_symbol", BIGINT_COLUMN));

        assertThat(converted).hasValue(format("(\"c_bigint\") %s (42)", operator.getValue()));
    }

    @DataProvider
    public static Object[][] testConvertArithmeticBinaryDataProvider()
    {
        return Stream.of(ArithmeticBinaryExpression.Operator.values())
                .collect(toDataProvider());
    }

    @Test
    public void testConvertArithmeticUnaryMinus()
    {
        Optional<String> converted = JDBC_CLIENT.convertPredicate(
                SESSION,
                translateToConnectorExpression(
                        new ArithmeticUnaryExpression(
                                ArithmeticUnaryExpression.Sign.MINUS,
                                new SymbolReference("c_bigint_symbol")),
                        Map.of("c_bigint_symbol", BIGINT)),
                Map.of("c_bigint_symbol", BIGINT_COLUMN));

        assertThat(converted).hasValue("-(\"c_bigint\")");
    }

    @Test
    public void testConvertLike()
    {
        // c_varchar LIKE '%pattern%'
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new LikePredicate(
                                new SymbolReference("c_varchar_symbol"),
                                new StringLiteral("%pattern%"),
                                Optional.empty()),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                Map.of("c_varchar_symbol", VARCHAR_COLUMN)))
                .hasValue("(\"c_varchar\") LIKE ('%pattern%')");

        // c_varchar LIKE '%pattern\%' ESCAPE '\'
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new LikePredicate(
                                new SymbolReference("c_varchar"),
                                new StringLiteral("%pattern\\%"),
                                new StringLiteral("\\")),
                        Map.of("c_varchar", VARCHAR_COLUMN.getColumnType())),
                Map.of(VARCHAR_COLUMN.getColumnName(), VARCHAR_COLUMN)))
                .hasValue("(\"c_varchar\") LIKE ('%pattern\\%') ESCAPE ('\\')");
    }

    @Test
    public void testConvertIsNull()
    {
        // c_varchar IS NULL
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new IsNullPredicate(
                                new SymbolReference("c_varchar_symbol")),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                Map.of("c_varchar_symbol", VARCHAR_COLUMN)))
                .hasValue("(\"c_varchar\") IS NULL");
    }

    @Test
    public void testConvertIsNotNull()
    {
        // c_varchar IS NOT NULL
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new IsNotNullPredicate(
                                new SymbolReference("c_varchar_symbol")),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                Map.of("c_varchar_symbol", VARCHAR_COLUMN)))
                .hasValue("(\"c_varchar\") IS NOT NULL");
    }

    @Test
    public void testConvertNullIf()
    {
        // nullif(a_varchar, b_varchar)
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new NullIfExpression(
                                new SymbolReference("a_varchar_symbol"),
                                new SymbolReference("b_varchar_symbol")),
                        ImmutableMap.of("a_varchar_symbol", VARCHAR_COLUMN.getColumnType(), "b_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                ImmutableMap.of("a_varchar_symbol", VARCHAR_COLUMN, "b_varchar_symbol", VARCHAR_COLUMN)))
                .hasValue("NULLIF((\"c_varchar\"), (\"c_varchar\"))");
    }

    @Test
    public void testConvertNotExpression()
    {
        // NOT(expression)
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new NotExpression(
                            new IsNotNullPredicate(
                                    new SymbolReference("c_varchar_symbol"))),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                Map.of("c_varchar_symbol", VARCHAR_COLUMN)))
                .hasValue("NOT ((\"c_varchar\") IS NOT NULL)");
    }

    @Test
    public void testConvertCoalesce()
    {
        // COALESCE(varchar, varchar)
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new CoalesceExpression(
                                new SymbolReference("c_varchar_symbol"),
                                new SymbolReference("c_varchar_symbol_2")),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType(), "c_varchar_symbol_2", VARCHAR_COLUMN.getColumnType())),
                Map.of("c_varchar_symbol", VARCHAR_COLUMN, "c_varchar_symbol_2", VARCHAR_COLUMN)))
                .hasValue("COALESCE(\"c_varchar\",\"c_varchar\")");

        // COALESCE(bigint, bigint, bigint)
        assertThat(JDBC_CLIENT.convertPredicate(SESSION,
                translateToConnectorExpression(
                        new CoalesceExpression(
                                new SymbolReference("c_bigint_symbol"),
                                new SymbolReference("c_bigint_symbol_2"),
                                new SymbolReference("c_bigint_symbol_3")),
                        Map.of("c_bigint_symbol", BIGINT_COLUMN.getColumnType(), "c_bigint_symbol_2", BIGINT_COLUMN.getColumnType(), "c_bigint_symbol_3", BIGINT_COLUMN.getColumnType())),
                Map.of("c_bigint_symbol", BIGINT_COLUMN, "c_bigint_symbol_2", BIGINT_COLUMN, "c_bigint_symbol_3", BIGINT_COLUMN)))
                .hasValue("COALESCE(\"c_bigint\",\"c_bigint\",\"c_bigint\")");
    }

    private ConnectorExpression translateToConnectorExpression(Expression expression, Map<String, Type> symbolTypes)
    {
        return ConnectorExpressionTranslator.translate(
                        TEST_SESSION,
                        expression,
                        createTestingTypeAnalyzer(PLANNER_CONTEXT),
                        TypeProvider.viewOf(symbolTypes.entrySet().stream()
                                .collect(toImmutableMap(entry -> new Symbol(entry.getKey()), Entry::getValue))),
                        PLANNER_CONTEXT)
                .orElseThrow();
    }
}
