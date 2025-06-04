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

import io.trino.plugin.base.mapping.DefaultIdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.ConnectorExpressionTranslator;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIgniteClient
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

    public static final JdbcClient JDBC_CLIENT = new IgniteClient(
            new BaseJdbcConfig(),
            session -> { throw new UnsupportedOperationException(); },
            new DefaultQueryBuilder(RemoteQueryModifier.NONE),
            new DefaultIdentifierMapping(),
            RemoteQueryModifier.NONE);

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
                Optional.of("count(`c_bigint`)"));

        // count(double)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("count(`c_double`)"));

        // count(DISTINCT bigint)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("count(DISTINCT `c_bigint`)"));

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
                Optional.of("sum(`c_bigint`)"));

        // sum(double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(`c_double`)"));

        // sum(DISTINCT bigint)
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("sum(DISTINCT `c_bigint`)"));

        // sum(DISTINCT double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(doubleVariable), List.of(), true, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(DISTINCT `c_double`)"));

        // sum(bigint) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("sum", BIGINT, List.of(bigintVariable), List.of(), false, filter),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty()); // filter not supported
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
        assertThat(converted.expression()).isEqualTo("(`c_varchar`) IS NULL");
        assertThat(converted.parameters()).isEmpty();
    }

    @Test
    public void testConvertIsNotNull()
    {
        // c_varchar IS NOT NULL
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                not(PLANNER_CONTEXT.getMetadata(), new IsNull(new Reference(VARCHAR, "c_varchar_symbol")))),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("(`c_varchar`) IS NOT NULL");
        assertThat(converted.parameters()).isEmpty();
    }

    @Test
    public void testConvertNotExpression()
    {
        // NOT(expression)
        ParameterizedExpression converted = JDBC_CLIENT.convertPredicate(SESSION,
                        translateToConnectorExpression(
                                not(
                                        PLANNER_CONTEXT.getMetadata(),
                                        not(PLANNER_CONTEXT.getMetadata(), new IsNull(new Reference(VARCHAR, "c_varchar_symbol"))))),
                        Map.of("c_varchar_symbol", VARCHAR_COLUMN))
                .orElseThrow();
        assertThat(converted.expression()).isEqualTo("NOT ((`c_varchar`) IS NOT NULL)");
        assertThat(converted.parameters()).isEmpty();
    }

    private ConnectorExpression translateToConnectorExpression(Expression expression)
    {
        return ConnectorExpressionTranslator.translate(TEST_SESSION, expression)
                .orElseThrow();
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
}
