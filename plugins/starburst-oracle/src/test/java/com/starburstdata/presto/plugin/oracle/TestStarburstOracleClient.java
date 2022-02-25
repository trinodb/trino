/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.starburstdata.presto.plugin.jdbc.redirection.TableScanRedirection;
import com.starburstdata.presto.plugin.jdbc.stats.JdbcStatisticsConfig;
import com.starburstdata.presto.redirection.NoneRedirectionsProvider;
import com.starburstdata.presto.redirection.RedirectionStats;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.TypeHandlingJdbcConfig;
import io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties;
import io.trino.plugin.jdbc.mapping.DefaultIdentifierMapping;
import io.trino.plugin.oracle.OracleConfig;
import io.trino.plugin.oracle.OracleSessionProperties;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.DecimalType;
import io.trino.testing.TestingConnectorSession;
import oracle.jdbc.OracleTypes;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStarburstOracleClient
{
    private static final JdbcColumnHandle DECIMAL_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_decimal")
                    .setColumnType(createDecimalType(21, 3))
                    .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.NUMBER, Optional.of("NUMBER"), 21, 3, Optional.empty()))
                    .build();

    private static final JdbcColumnHandle DOUBLE_COLUMN =
            JdbcColumnHandle.builder()
                    .setColumnName("c_double")
                    .setColumnType(DOUBLE)
                    .setJdbcTypeHandle(new JdbcTypeHandle(OracleTypes.BINARY_DOUBLE, Optional.of("BINARY DOUBLE"), 0, 0, Optional.empty()))
                    .build();

    private static final JdbcClient JDBC_CLIENT = new StarburstOracleClient(
            NOOP_LICENSE_MANAGER,
            new BaseJdbcConfig(),
            new JdbcMetadataConfig().setAggregationPushdownEnabled(true),
            new JdbcStatisticsConfig(),
            new TableScanRedirection(new NoneRedirectionsProvider(), NOOP_LICENSE_MANAGER, new RedirectionStats()),
            new OracleConfig(),
            session -> { throw new UnsupportedOperationException(); },
            new DefaultQueryBuilder(),
            new DefaultIdentifierMapping());

    public static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(ImmutableList.<PropertyMetadata<?>>builder()
                    .addAll(new TypeHandlingJdbcSessionProperties(new TypeHandlingJdbcConfig()).getSessionProperties())
                    .addAll(new OracleSessionProperties(new OracleConfig()).getSessionProperties())
                    .addAll(new StarburstOracleSessionProperties(NOOP_LICENSE_MANAGER, new StarburstOracleConfig()).getSessionProperties())
                    .build())
            .build();

    @Test
    public void testImplementCount()
    {
        Variable decimalVariable = new Variable("v_decimal", createDecimalType(19, 0));
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // count(*)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, Optional.empty()),
                Map.of(),
                Optional.of("count(*)"));

        // count(decimal)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(decimalVariable), List.of(), false, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("count(\"c_decimal\")"));

        // count(double)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("count(\"c_double\")"));

        // count(DISTINCT decimal)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(decimalVariable), List.of(), true, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("count(DISTINCT \"c_decimal\")"));

        // count() FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(), List.of(), false, filter),
                Map.of(),
                Optional.empty());

        // count(decimal) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("count", BIGINT, List.of(decimalVariable), List.of(), false, filter),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.empty());
    }

    @Test
    public void testImplementSum()
    {
        DecimalType decimalType = createDecimalType(21, 3);
        DecimalType decimalSumType = createDecimalType(38, 3);
        Variable decimalVariable = new Variable("v_decimal", decimalType);
        Variable doubleVariable = new Variable("v_double", DOUBLE);
        Optional<ConnectorExpression> filter = Optional.of(new Variable("a_filter", BOOLEAN));

        // sum(decimal)
        testImplementAggregation(
                new AggregateFunction("sum", decimalSumType, List.of(decimalVariable), List.of(), false, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.of("sum(\"c_decimal\")"));

        // sum(double)
        testImplementAggregation(
                new AggregateFunction("sum", DOUBLE, List.of(doubleVariable), List.of(), false, Optional.empty()),
                Map.of(doubleVariable.getName(), DOUBLE_COLUMN),
                Optional.of("sum(\"c_double\")"));

        // sum(DISTINCT decimal)
        testImplementAggregation(
                new AggregateFunction("sum", decimalSumType, List.of(decimalVariable), List.of(), true, Optional.empty()),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.empty());  // distinct not supported

        // sum(decimal) FILTER (WHERE ...)
        testImplementAggregation(
                new AggregateFunction("sum", decimalSumType, List.of(decimalVariable), List.of(), false, filter),
                Map.of(decimalVariable.getName(), DECIMAL_COLUMN),
                Optional.empty()); // filter not supported
    }

    private void testImplementAggregation(AggregateFunction aggregateFunction, Map<String, ColumnHandle> assignments, Optional<String> expectedExpression)
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
