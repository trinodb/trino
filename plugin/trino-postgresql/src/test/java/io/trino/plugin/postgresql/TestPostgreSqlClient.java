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
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcMetadataSessionProperties;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.DatabaseMetaDataRemoteIdentifierSupplier;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
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

    private static final JdbcColumnHandle VARCHAR_COLUMN2 =
            JdbcColumnHandle.builder()
                    .setColumnName("c_varchar2")
                    .setColumnType(createVarcharType(10))
                    .setJdbcTypeHandle(new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), Optional.of(10), Optional.empty(), Optional.empty(), Optional.empty()))
                    .build();

    private static final JdbcClient JDBC_CLIENT = new PostgreSqlClient(
            new BaseJdbcConfig(),
            new PostgreSqlConfig(),
            new JdbcStatisticsConfig(),
            session -> { throw new UnsupportedOperationException(); },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            TESTING_TYPE_MANAGER,
            new DefaultIdentifierMapping(new DatabaseMetaDataRemoteIdentifierSupplier()),
            RemoteQueryModifier.NONE);

    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(PLANNER_CONTEXT);

    private static final ConnectorSession SESSION = TestingConnectorSession
            .builder()
            .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
            .build();

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
                Optional.of("sum(DISTINCT \"c_bigint\")"));

        // sum(DISTINCT double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(DISTINCT \"c_double\")"));

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
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
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
                                "c_bigint_symbol_2", BIGINT_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("((\"c_bigint\") = (?)) OR ((\"c_bigint\") = (?))");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(BIGINT, Optional.of(42L)),
                new QueryParameter(BIGINT, Optional.of(415L))));
    }

    @Test
    public void testConvertOrWithAnd()
    {
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new LogicalExpression(
                                        LogicalExpression.Operator.OR,
                                        List.of(
                                                new ComparisonExpression(
                                                        ComparisonExpression.Operator.EQUAL,
                                                        new SymbolReference("c_bigint_symbol"),
                                                        LITERAL_ENCODER.toExpression(TEST_SESSION, 42L, BIGINT)),
                                                new LogicalExpression(
                                                        LogicalExpression.Operator.AND,
                                                        List.of(
                                                                new ComparisonExpression(
                                                                        ComparisonExpression.Operator.EQUAL,
                                                                        new SymbolReference("c_bigint_symbol"),
                                                                        LITERAL_ENCODER.toExpression(TEST_SESSION, 43L, BIGINT)),
                                                                new ComparisonExpression(
                                                                        ComparisonExpression.Operator.EQUAL,
                                                                        new SymbolReference("c_bigint_symbol_2"),
                                                                        LITERAL_ENCODER.toExpression(TEST_SESSION, 44L, BIGINT)))))),
                                Map.of(
                                        "c_bigint_symbol", BIGINT,
                                        "c_bigint_symbol_2", BIGINT)),
                        Map.of(
                                "c_bigint_symbol", BIGINT_COLUMN,
                                "c_bigint_symbol_2", BIGINT_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("((\"c_bigint\") = (?)) OR (((\"c_bigint\") = (?)) AND ((\"c_bigint\") = (?)))");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(BIGINT, Optional.of(42L)),
                new QueryParameter(BIGINT, Optional.of(43L)),
                new QueryParameter(BIGINT, Optional.of(44L))));
    }

    @Test(dataProvider = "testConvertComparisonDataProvider")
    public void testConvertComparison(ComparisonExpression.Operator operator)
    {
        Optional<ParameterizedExpression> converted = JDBC_CLIENT.convertPredicate(
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
                assertThat(converted).isPresent();
                assertThat(converted.get().expression()).isEqualTo(format("(\"c_bigint\") %s (?)", operator.getValue()));
                assertThat(converted.get().parameters()).isEqualTo(List.of(new QueryParameter(BIGINT, Optional.of(42L))));
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
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new ArithmeticBinaryExpression(
                                        operator,
                                        new SymbolReference("c_bigint_symbol"),
                                        LITERAL_ENCODER.toExpression(TEST_SESSION, 42L, BIGINT)),
                                Map.of("c_bigint_symbol", BIGINT)),
                        Map.of("c_bigint_symbol", BIGINT_COLUMN))
                .orElseThrow();

        assertThat(converted.expression()).isEqualTo(format("(\"c_bigint\") %s (?)", operator.getValue()));
        assertThat(converted.parameters()).isEqualTo(List.of(new QueryParameter(BIGINT, Optional.of(42L))));
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
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new ArithmeticUnaryExpression(
                                        ArithmeticUnaryExpression.Sign.MINUS,
                                        new SymbolReference("c_bigint_symbol")),
                                Map.of("c_bigint_symbol", BIGINT)),
                        Map.of("c_bigint_symbol", BIGINT_COLUMN))
                .orElseThrow();

        assertThat(converted.expression()).isEqualTo("-(\"c_bigint\")");
        assertThat(converted.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testConvertLike()
    {
        // c_varchar LIKE '%pattern%'
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new LikePredicate(
                                        new SymbolReference("c_varchar_symbol"),
                                        new StringLiteral("%pattern%"),
                                        Optional.empty()),
                                Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(\"c_varchar\") LIKE (?)");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(createVarcharType(9), Optional.of(utf8Slice("%pattern%")))));

        // c_varchar LIKE '%pattern\%' ESCAPE '\'
        converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new LikePredicate(
                                        new SymbolReference("c_varchar"),
                                        new StringLiteral("%pattern\\%"),
                                        new StringLiteral("\\")),
                                Map.of("c_varchar", VARCHAR_COLUMN.getColumnType())),
                        Map.of(VARCHAR_COLUMN.getColumnName(), VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(\"c_varchar\") LIKE (?) ESCAPE (?)");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(createVarcharType(10), Optional.of(utf8Slice("%pattern\\%"))),
                new QueryParameter(createVarcharType(1), Optional.of(utf8Slice("\\")))));
    }

    @Test
    public void testConvertIsNull()
    {
        // c_varchar IS NULL
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new IsNullPredicate(
                                        new SymbolReference("c_varchar_symbol")),
                                Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(\"c_varchar\") IS NULL");
        assertThat(converted.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testConvertIsNotNull()
    {
        // c_varchar IS NOT NULL
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new IsNotNullPredicate(
                                        new SymbolReference("c_varchar_symbol")),
                                Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(\"c_varchar\") IS NOT NULL");
        assertThat(converted.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testConvertNullIf()
    {
        // nullif(a_varchar, b_varchar)
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new NullIfExpression(
                                        new SymbolReference("a_varchar_symbol"),
                                        new SymbolReference("b_varchar_symbol")),
                                ImmutableMap.of("a_varchar_symbol", VARCHAR_COLUMN.getColumnType(), "b_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                        ImmutableMap.of("a_varchar_symbol", VARCHAR_COLUMN, "b_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("NULLIF((\"c_varchar\"), (\"c_varchar\"))");
        assertThat(converted.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testConvertNotExpression()
    {
        // NOT(expression)
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new NotExpression(
                                        new IsNotNullPredicate(
                                                new SymbolReference("c_varchar_symbol"))),
                                Map.of("c_varchar_symbol", VARCHAR_COLUMN.getColumnType())),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("NOT ((\"c_varchar\") IS NOT NULL)");
        assertThat(converted.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testConvertIn()
    {
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new InPredicate(
                                        new SymbolReference("c_varchar"),
                                        new InListExpression(List.of(new StringLiteral("value1"), new StringLiteral("value2"), new SymbolReference("c_varchar2")))),
                                Map.of("c_varchar", VARCHAR_COLUMN.getColumnType(), "c_varchar2", VARCHAR_COLUMN2.getColumnType())),
                        Map.of(VARCHAR_COLUMN.getColumnName(), VARCHAR_COLUMN, VARCHAR_COLUMN2.getColumnName(), VARCHAR_COLUMN2))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(\"c_varchar\") IN (?, ?, \"c_varchar2\")");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(createVarcharType(6), Optional.of(utf8Slice("value1"))),
                new QueryParameter(createVarcharType(6), Optional.of(utf8Slice("value2")))));
    }

    private ConnectorExpression translateToConnectorExpression(Expression expression, Map<String, Type> symbolTypes)
    {
        return ConnectorExpressionTranslator.translate(
                        TEST_SESSION,
                        expression,
                        TypeProvider.viewOf(symbolTypes.entrySet().stream()
                                .collect(toImmutableMap(entry -> new Symbol(entry.getKey()), Entry::getValue))),
                        PLANNER_CONTEXT,
                        createTestingTypeAnalyzer(PLANNER_CONTEXT))
                .orElseThrow();
    }
}
