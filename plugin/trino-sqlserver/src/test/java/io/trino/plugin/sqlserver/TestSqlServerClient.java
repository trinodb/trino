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
package io.trino.plugin.sqlserver;

import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class TestSqlServerClient
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

    private static final JdbcClient JDBC_CLIENT = new SqlServerClient(
            new BaseJdbcConfig(),
            new SqlServerConfig(),
            session -> {
                throw new UnsupportedOperationException();
            },
            new DefaultIdentifierMapping());

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
                Optional.of("count_big(*)"));

        // count(bigint)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), false, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.of("count_big(\"c_bigint\")"));

        // count(double)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("count_big(\"c_double\")"));

        // count(DISTINCT bigint)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(bigintVariable), List.of(), true, Optional.empty()),
                Map.of(bigintVariable.getName(), BIGINT_COLUMN),
                Optional.empty());

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
}
