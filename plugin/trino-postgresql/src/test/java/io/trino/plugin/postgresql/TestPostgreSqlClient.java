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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
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
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.OperatorType;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPostgreSqlClient
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));

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
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

    private static final ConnectorSession SESSION = TestingConnectorSession
            .builder()
            .setPropertyMetadata(ImmutableList.<PropertyMetadata<?>>builder()
                    .addAll(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
                    .addAll(new PostgreSqlSessionProperties(new PostgreSqlConfig()).getSessionProperties())
                    .build())
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
            assertThat(result.get().getExpression()).isEqualTo(expectedExpression.get());
            Optional<ColumnMapping> columnMapping = JDBC_CLIENT.toColumnMapping(SESSION, null, result.get().getJdbcTypeHandle());
            assertThat(columnMapping.isPresent())
                    .describedAs("No mapping for: " + result.get().getJdbcTypeHandle())
                    .isTrue();
            assertThat(columnMapping.get().getType()).isEqualTo(aggregateFunction.getOutputType());
        }
    }

    @Test
    public void testConvertOr()
    {
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new Logical(
                                        Logical.Operator.OR,
                                        List.of(
                                                new Comparison(Comparison.Operator.EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L)),
                                                new Comparison(Comparison.Operator.EQUAL, new Reference(BIGINT, "c_bigint_symbol_2"), new Constant(BIGINT, 415L))))),
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
                                new Logical(
                                        Logical.Operator.OR,
                                        List.of(
                                                new Comparison(Comparison.Operator.EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L)),
                                                new Logical(
                                                        Logical.Operator.AND,
                                                        List.of(
                                                                new Comparison(Comparison.Operator.EQUAL, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 43L)),
                                                                new Comparison(Comparison.Operator.EQUAL, new Reference(BIGINT, "c_bigint_symbol_2"), new Constant(BIGINT, 44L))))))),
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

    @Test
    public void testConvertComparison()
    {
        for (Comparison.Operator operator : Comparison.Operator.values()) {
            Optional<ParameterizedExpression> converted = JDBC_CLIENT.convertPredicate(
                    SESSION,
                    translateToConnectorExpression(
                            new Comparison(operator, new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L))),
                    Map.of("c_bigint_symbol", BIGINT_COLUMN));

            switch (operator) {
                case EQUAL:
                case NOT_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case IS_DISTINCT_FROM:
                    assertThat(converted).isPresent();
                    assertThat(converted.get().expression()).isEqualTo(format("(\"c_bigint\") %s (?)", operator.getValue()));
                    assertThat(converted.get().parameters()).isEqualTo(List.of(new QueryParameter(BIGINT, Optional.of(42L))));
                    break;
            }
        }
    }

    @Test
    public void testConvertArithmeticBinary()
    {
        TestingFunctionResolution resolver = new TestingFunctionResolution();

        for (OperatorType operator : EnumSet.of(ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULUS)) {
            ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                            SESSION,
                            translateToConnectorExpression(
                                    new Call(resolver.resolveOperator(
                                            operator,
                                            ImmutableList.of(BIGINT, BIGINT)), ImmutableList.of(new Reference(BIGINT, "c_bigint_symbol"), new Constant(BIGINT, 42L)))),
                            Map.of("c_bigint_symbol", BIGINT_COLUMN))
                    .orElseThrow();

            assertThat(converted.parameters()).isEqualTo(List.of(new QueryParameter(BIGINT, Optional.of(42L))));
        }
    }

    @Test
    public void testConvertArithmeticUnaryMinus()
    {
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(
                        SESSION,
                        translateToConnectorExpression(
                                new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "c_bigint_symbol")))),
                        Map.of("c_bigint_symbol", BIGINT_COLUMN))
                .orElseThrow();

        assertThat(converted.expression()).isEqualTo("-(\"c_bigint\")");
        assertThat(converted.parameters()).isEqualTo(List.of());
    }

    @Test
    public void testConvertIsNull()
    {
        // c_varchar IS NULL
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                new IsNull(
                                        new Reference(VARCHAR, "c_varchar_symbol"))),
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
                                new Not(new IsNull(new Reference(VARCHAR, "c_varchar_symbol")))),
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
                                new NullIf(
                                        new Reference(VARCHAR, "a_varchar_symbol"),
                                        new Reference(VARCHAR, "b_varchar_symbol"))),
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
                                new Not(
                                        new Not(new IsNull(new Reference(VARCHAR, "c_varchar_symbol"))))),
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
                                new In(
                                        new Reference(createVarcharType(10), "c_varchar"),
                                        List.of(
                                                new Constant(VARCHAR_COLUMN.getColumnType(), utf8Slice("value1")),
                                                new Constant(VARCHAR_COLUMN.getColumnType(), utf8Slice("value2")),
                                                new Reference(createVarcharType(10), "c_varchar2")))),
                        Map.of(VARCHAR_COLUMN.getColumnName(), VARCHAR_COLUMN, VARCHAR_COLUMN2.getColumnName(), VARCHAR_COLUMN2))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(\"c_varchar\") IN (?, ?, \"c_varchar2\")");
        assertThat(converted.parameters()).isEqualTo(List.of(
                new QueryParameter(createVarcharType(10), Optional.of(utf8Slice("value1"))),
                new QueryParameter(createVarcharType(10), Optional.of(utf8Slice("value2")))));
    }

    private ConnectorExpression translateToConnectorExpression(Expression expression)
    {
        return ConnectorExpressionTranslator.translate(TEST_SESSION, expression)
                .orElseThrow();
    }
}
