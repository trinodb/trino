/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.dynamodb.testing.DynamoDbTemporaryTable;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.predicate.Domain.DEFAULT_COMPACTION_THRESHOLD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDynamoDbPredicatePushdown
        extends AbstractTestQueryFramework
{
    private TestingDynamoDbServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new TestingDynamoDbServer());

        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                .setTables(List.of()) // None of the tpch tables are required in the tests of this class.
                .addConnectorProperties(
                        Map.of("dynamodb.generate-schema-files", "ON_START", // Set it to the same value as that of set in Galaxy config.
                                "dynamodb.extra-jdbc-properties", "Other=\"SchemaCacheDuration=0\"")) // Don't cache table metadata. Required for created test table to show up.
                .enablePredicatePushdown()
                .enableWrites()
                .build();
    }

    @Test
    public void testBooleanPushdown()
    {
        String tableName = "test_bool";
        try (DynamoDbTemporaryTable table
                = new DynamoDbTemporaryTable(
                        server.getEndpointUrl(),
                "test_bool",
                        "row_id",
                        KeyType.HASH,
                        ScalarAttributeType.S,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        List.of(
                                ImmutableMap.of(
                                        "row_id", AttributeValue.builder().s("a").build(),
                                        "col1", AttributeValue.builder().bool(Boolean.TRUE).build()),
                                ImmutableMap.of(
                                        "row_id", AttributeValue.builder().s("b").build(),
                                        "col1", AttributeValue.builder().bool(Boolean.FALSE).build()),
                                ImmutableMap.of(
                                        "row_id", AttributeValue.builder().s("c").build(),
                                        "col2", AttributeValue.builder().bool(Boolean.TRUE).build())))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("""
                        CREATE TABLE dynamodb.amazondynamodb.%s (
                           row_id varchar(255) NOT NULL COMMENT 'Dynamic Column.',
                           col1 boolean COMMENT 'Dynamic Column.',
                           col2 boolean COMMENT 'Dynamic Column.'
                        )""".formatted(tableName));

            assertQuery(
                    "SELECT row_id, col1, col2 FROM " + tableName,
                    "VALUES ('a', true, null), ('b', false, null), ('c', null, true)");

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (true, null)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (true, null)"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BOOLEAN, 16, "BIT", Optional.of(5), Optional.empty()),
                                                        Domain.create(ValueSet.ofRanges(Range.equal(BOOLEAN, true)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (true, null)"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= true"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 <= false"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= true AND col1 <= false"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= true OR col1 <= null"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 = true"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 = null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != true"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != null"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IS NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IS NOT NULL"))
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE NOT col1"))
                    .isNotFullyPushedDown(FilterNode.class);

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 BETWEEN true AND false"))
                    .isReplacedWithEmptyValues();
        }
    }

    @Test
    public void testIntegerPushdown()
    {
        String tableName = "test_int";
        String primaryKey = "row_id";
        String col1 = "col1";
        String col2 = "col2";

        try (DynamoDbTemporaryTable table
                = new DynamoDbTemporaryTable(
                        server.getEndpointUrl(),
                        tableName,
                        primaryKey,
                        KeyType.HASH,
                        ScalarAttributeType.S,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        List.of(
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("a").build(),
                                        col1, AttributeValue.builder().n("11111").build()),
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("b").build(),
                                        col1, AttributeValue.builder().n("22222").build()),
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("c").build(),
                                        col2, AttributeValue.builder().n("33333").build())))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("""
                            CREATE TABLE dynamodb.amazondynamodb.%s (
                               %s varchar(255) NOT NULL COMMENT 'Dynamic Column.',
                               %s integer COMMENT 'Dynamic Column.',
                               %s integer COMMENT 'Dynamic Column.'
                            )""".formatted(tableName, primaryKey, col1, col2));

            assertQuery(
                    "SELECT row_id, col1, col2 FROM " + tableName,
                    "VALUES ('a', 11111, null), ('b', 22222, null), ('c', null, 33333)");

            // Asserts on non-primary key column
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (11111, 22222)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (null, 22222)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (11111, 22222)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (null, 11111, 22222)"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 AND col1 IS NULL"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 AND col1 IS NOT NULL"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 AND col1 IS NOT NULL"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", INTEGER, 4,"INT", Optional.of(10), Optional.of(10)),
                                                        Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 11111L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 11111 AND col1 <= null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= null AND col1 <= 11111"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 AND col1 < null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > null AND col1 < 11111"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 11111 AND col1 <= 22222"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE NOT (col1 >= 11111 AND col1 <= 22222)"))
                    .isFullyPushedDown();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 OR col1 IS NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 OR col1 IS NOT NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 11111 OR col1 <= null"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 11111 OR col1 <= null"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", INTEGER, 4,"INT", Optional.of(10), Optional.of(10)),
                                                        Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(INTEGER, 11111L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= null OR col1 <= 11111"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= null OR col1 <= 11111"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", INTEGER, 4,"INT", Optional.of(10), Optional.of(10)),
                                                        Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(INTEGER, 11111L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 OR col1 < null"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111 OR col1 < null"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", INTEGER, 4,"INT", Optional.of(10), Optional.of(10)),
                                                        Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 11111L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > null OR col1 < 11111"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > null OR col1 < 11111"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", INTEGER, 4,"INT", Optional.of(10), Optional.of(10)),
                                                        Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 11111L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 11111 OR col1 <= 22222"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE NOT (col1 >= 11111 OR col1 <= 22222)"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 11111"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 11111"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 < 22222"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 <= 22222"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 = 22222"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 = null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != 22222"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != 22222"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", INTEGER, 4,"INT", Optional.of(10), Optional.of(10)),
                                                        Domain.create(ValueSet.ofRanges(Range.lessThan(INTEGER, 22222L), Range.greaterThan(INTEGER, 22222L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != null"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IS NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IS NOT NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 BETWEEN 1 AND 22222"))
                    .isFullyPushedDown();

            // Test different values lesser than or equals to `DEFAULT_COMPACTION_THRESHOLD`
            for (int count : largeInValuesCountData()) {
                String longValues = range(0, count)
                        .mapToObj(Integer::toString)
                        .collect(joining(", "));
                String longValuesMinusOne = range(0, count - 1) // Used for NOT IN clause as it generates (NOT IN parameters + 1) ranges which exceeds the `DEFAULT_COMPACTION_THRESHOLD` when tested with value equals to `DEFAULT_COMPACTION_THRESHOLD`
                        .mapToObj(Integer::toString)
                        .collect(joining(", "));

                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (" + longValues + ")"))
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (" + longValuesMinusOne + ")"))
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (mod(1000, col1), " + longValuesMinusOne + ")"))
                        .isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (mod(1000, col2), " + longValues + ")"))
                        .isNotFullyPushedDown(FilterNode.class);
            }

            // Test with (`DEFAULT_COMPACTION_THRESHOLD` + 1) to confirm that predicates are not pushed down
            String crossesThreshold = range(0, DEFAULT_COMPACTION_THRESHOLD + 1)
                    .mapToObj(Integer::toString)
                    .collect(joining(", "));
            String threshold = range(0, DEFAULT_COMPACTION_THRESHOLD) // Used for NOT IN clause as it generates (NOT IN parameters + 1) ranges which exceeds the `DEFAULT_COMPACTION_THRESHOLD` threshold
                    .mapToObj(Integer::toString)
                    .collect(joining(", "));

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (" + crossesThreshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (" + threshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (mod(1000, col1), " + threshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (mod(1000, col2), " + crossesThreshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testBigintPushdown()
    {
        String tableName = "test_bigint";
        String primaryKey = "row_id";
        String col1 = "col1";
        String col2 = "col2";

        try (DynamoDbTemporaryTable table
                = new DynamoDbTemporaryTable(
                        server.getEndpointUrl(),
                tableName,
                primaryKey,
                KeyType.HASH,
                ScalarAttributeType.S,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                List.of(
                        ImmutableMap.of(
                                primaryKey, AttributeValue.builder().s("a").build(),
                                col1, AttributeValue.builder().n("9223372036854775806").build()),
                        ImmutableMap.of(
                                primaryKey, AttributeValue.builder().s("b").build(),
                                col1, AttributeValue.builder().n("9223372036854775805").build()),
                        ImmutableMap.of(
                                primaryKey, AttributeValue.builder().s("c").build(),
                                col2, AttributeValue.builder().n("9223372036854775804").build())))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("""
                            CREATE TABLE dynamodb.amazondynamodb.%s (
                               %s varchar(255) NOT NULL COMMENT 'Dynamic Column.',
                               %s bigint COMMENT 'Dynamic Column.',
                               %s bigint COMMENT 'Dynamic Column.'
                            )""".formatted(tableName, primaryKey, col1, col2));

            assertQuery(
                    "SELECT row_id, col1, col2 FROM " + tableName,
                    "VALUES ('a', 9223372036854775806, null), ('b', 9223372036854775805, null), ('c', null, 9223372036854775804)");

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (9223372036854775805, 9223372036854775804)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (null, 9223372036854775805)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (9223372036854775805, 9223372036854775804)"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (null, 9223372036854775805, 9223372036854775804)"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 AND col1 IS NULL"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 AND col1 IS NOT NULL"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 AND col1 IS NOT NULL"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BIGINT, -5,"BIGINT", Optional.of(19), Optional.of(0)),
                                                        Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 9223372036854775805L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 9223372036854775805 AND col1 <= null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 AND col1 < null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= null AND col1 <= 9223372036854775806"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > null AND col1 < 9223372036854775806"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 9223372036854775805 AND col1 <= 9223372036854775806"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE NOT (col1 >= 9223372036854775805 AND col1 <= 9223372036854775806)"))
                    .isFullyPushedDown();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 OR col1 IS NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 OR col1 IS NOT NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 9223372036854775805 OR col1 <= null"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 9223372036854775805 OR col1 <= null"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BIGINT, -5,"BIGINT", Optional.of(19), Optional.of(0)),
                                                        Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, 9223372036854775805L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= null OR col1 <= 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= null OR col1 <= 9223372036854775805"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BIGINT, -5,"BIGINT", Optional.of(19), Optional.of(0)),
                                                        Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 9223372036854775805L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 OR col1 < null"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805 OR col1 < null"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BIGINT, -5,"BIGINT", Optional.of(19), Optional.of(0)),
                                                        Domain.create(ValueSet.ofRanges(Range.greaterThan(BIGINT, 9223372036854775805L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > null OR col1 < 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > null OR col1 < 9223372036854775805"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BIGINT, -5,"BIGINT", Optional.of(19), Optional.of(0)),
                                                        Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 9223372036854775805L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 9223372036854775805 OR col1 <= 9223372036854775806"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE NOT (col1 >= 9223372036854775805 OR col1 <= 9223372036854775806)"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 > 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 >= 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 < 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 <= 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 = 9223372036854775805"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 = null"))
                    .isReplacedWithEmptyValues();
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != 9223372036854775805"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != 9223372036854775805"))
                    .isNotFullyPushedDown(node(TableScanNode.class)
                            .with(TableScanNode.class, tableScanNode -> {
                                TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                                TupleDomain<?> expectedPredicate =
                                        TupleDomain.withColumnDomains(
                                                Map.of(
                                                        createColumnHandle("col1", BIGINT, -5,"BIGINT", Optional.of(19), Optional.of(0)),
                                                        Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 9223372036854775805L), Range.greaterThan(BIGINT, 9223372036854775805L)), false))); // Shows what predicate is pushed down for the above query
                                assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                                return true;
                            }));
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 != null"))
                    .isReplacedWithEmptyValues();

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IS NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IS NOT NULL"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 BETWEEN 1 AND 9223372036854775806"))
                    .isFullyPushedDown();

            // Test different values lesser than or equals to `DEFAULT_COMPACTION_THRESHOLD`
            for (int count : largeInValuesCountData()) {
                String longValues = range(0, count)
                        .mapToObj(Integer::toString)
                        .collect(joining(", "));
                String longValuesMinusOne = range(0, count - 1) // Used for NOT IN clause as it generates (NOT IN parameters + 1) ranges which exceeds the `DEFAULT_COMPACTION_THRESHOLD` when tested with value equals to `DEFAULT_COMPACTION_THRESHOLD`
                        .mapToObj(Integer::toString)
                        .collect(joining(", "));

                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (" + longValues + ")"))
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (" + longValuesMinusOne + ")"))
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (mod(1000, col1), " + longValuesMinusOne + ")"))
                        .isNotFullyPushedDown(FilterNode.class);
                assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (mod(1000, col2), " + longValues + ")"))
                        .isNotFullyPushedDown(FilterNode.class);
            }

            // Test with (`DEFAULT_COMPACTION_THRESHOLD` + 1) to confirm that predicates are not pushed down
            String crossesThreshold = range(0, DEFAULT_COMPACTION_THRESHOLD + 1)
                    .mapToObj(Integer::toString)
                    .collect(joining(", "));
            String threshold = range(0, DEFAULT_COMPACTION_THRESHOLD) // Used for NOT IN clause as it generates (NOT IN parameters + 1) ranges which exceeds the `DEFAULT_COMPACTION_THRESHOLD` threshold
                    .mapToObj(Integer::toString)
                    .collect(joining(", "));

            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (" + crossesThreshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (" + threshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 NOT IN (mod(1000, col1), " + threshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE col1 IN (mod(1000, col2), " + crossesThreshold + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
        }
    }

    @Test
    public void testVarcharPushdown()
    {
        String tableName = "test_varchar";

        String primaryKey = "row_id";
        KeyType primaryType = KeyType.HASH;
        ScalarAttributeType primaryKeyDataType = ScalarAttributeType.S;

        String sortKey = "sort_key";
        KeyType sortKeyType = KeyType.RANGE;
        ScalarAttributeType sortKeyDataType = ScalarAttributeType.S;

        String col1 = "col1";
        String col2 = "col2";

        try (DynamoDbTemporaryTable table
                = new DynamoDbTemporaryTable(
                        server.getEndpointUrl(),
                        tableName,
                        primaryKey,
                        primaryType,
                        primaryKeyDataType,
                        Optional.of(sortKey),
                        Optional.of(sortKeyType),
                        Optional.of(sortKeyDataType),
                        List.of(
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("a").build(),
                                        sortKey, AttributeValue.builder().s("aaa").build(),
                                        col1, AttributeValue.builder().s("value1").build()),
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("b").build(),
                                        sortKey, AttributeValue.builder().s("bbb").build(),
                                        col1, AttributeValue.builder().s("value2").build()),
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("c").build(),
                                        sortKey, AttributeValue.builder().s("ccc").build(),
                                        col2, AttributeValue.builder().s("value3").build())))) {
            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                .isEqualTo("""
                        CREATE TABLE dynamodb.amazondynamodb.%s (
                           %s varchar(255) NOT NULL COMMENT 'Dynamic Column.',
                           %s varchar(255) NOT NULL COMMENT 'Dynamic Column.',
                           %s varchar(2000) COMMENT 'Dynamic Column.',
                           %s varchar(2000) COMMENT 'Dynamic Column.'
                        )""".formatted(tableName, sortKey, primaryKey, col1, col2));
            assertQuery(
                "SELECT row_id, sort_key, col1, col2 FROM " + tableName,
                "VALUES ('a', 'aaa', 'value1', null), ('b', 'bbb', 'value2', null), ('c', 'ccc', null, 'value3')");

            testVarcharPushdown(tableName, primaryKey, 255, "VARCHAR", "a", "b");
            testVarcharPushdown(tableName, sortKey, 255, "VARCHAR", "aaa", "bbb");
            testVarcharPushdown(tableName, col1, 2000, "VARCHAR(2000)", "value1", "value2");
        }
    }

    private void testVarcharPushdown(String tableName, String columnName, int columnPrecision, String jdbcTypeName, String value1, String value2)
    {
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IN ('" + value1 + "', '" + value2 + "')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IN (null, '" + value1 + "')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " NOT IN ('" + value1 + " ', '" + value2 + "')"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " NOT IN (null, '" + value1 + "', '" + value2 + "')"))
                .isReplacedWithEmptyValues();

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' AND " + columnName + " IS NULL"))
                .isReplacedWithEmptyValues();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' AND " + columnName + " IS NOT NULL"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' AND " + columnName + " IS NOT NULL"))
                .isNotFullyPushedDown(node(TableScanNode.class)
                        .with(TableScanNode.class, tableScanNode -> {
                            TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                            TupleDomain<?> expectedPredicate =
                                    TupleDomain.withColumnDomains(
                                            Map.of(
                                                    createColumnHandle(columnName, createVarcharType(columnPrecision), 12, jdbcTypeName, Optional.empty(), Optional.empty()),
                                                    Domain.create(ValueSet.ofRanges(Range.greaterThan(createVarcharType(columnPrecision), value1)), false))); // Shows what predicate is pushed down for the above query
                            assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                            return true;
                        }));

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= '" + value1 + "' AND col1 <= null"))
                .isReplacedWithEmptyValues();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= null AND " + columnName + " <= '" + value1 + "'"))
                .isReplacedWithEmptyValues();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' AND " + columnName + " < null"))
                .isReplacedWithEmptyValues();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > null AND " + columnName + " < '" + value1 + "'"))
                .isReplacedWithEmptyValues();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= '" + value1 + "' AND " + columnName + " <= '" + value2 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE NOT (" + columnName + " >= '" + value1 + "' AND " + columnName + " <= '" + value2 + "')"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' OR " + columnName + " IS NULL"))
                .isNotFullyPushedDown(node(TableScanNode.class));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' OR " + columnName + " IS NOT NULL"))
                .isNotFullyPushedDown(node(TableScanNode.class));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= '" + value1 + "' OR " + columnName + " <= null"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= '" + value1 + "' OR " + columnName + " <= null"))
                .isNotFullyPushedDown(node(TableScanNode.class)
                        .with(TableScanNode.class, tableScanNode -> {
                            TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                            TupleDomain<?> expectedPredicate =
                                    TupleDomain.withColumnDomains(
                                            Map.of(
                                                    createColumnHandle(columnName, createVarcharType(columnPrecision), 12, jdbcTypeName, Optional.empty(), Optional.empty()),
                                                    Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(createVarcharType(columnPrecision), value1)), false))); // Shows what predicate is pushed down for the above query
                            assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                            return true;
                        }));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= null OR " + columnName + " <= '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= null OR " + columnName + " <= '" + value1 + "'"))
                .isNotFullyPushedDown(node(TableScanNode.class)
                        .with(TableScanNode.class, tableScanNode -> {
                            TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                            TupleDomain<?> expectedPredicate =
                                    TupleDomain.withColumnDomains(
                                            Map.of(
                                                    createColumnHandle(columnName, createVarcharType(columnPrecision), 12, jdbcTypeName, Optional.empty(), Optional.empty()),
                                                    Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(createVarcharType(columnPrecision), value1)), false))); // Shows what predicate is pushed down for the above query
                            assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                            return true;
                        }));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' OR " + columnName + " < null"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "' OR " + columnName + " < null"))
                .isNotFullyPushedDown(node(TableScanNode.class)
                        .with(TableScanNode.class, tableScanNode -> {
                            TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                            TupleDomain<?> expectedPredicate =
                                    TupleDomain.withColumnDomains(
                                            Map.of(
                                                    createColumnHandle(columnName, createVarcharType(columnPrecision), 12, jdbcTypeName, Optional.empty(), Optional.empty()),
                                                    Domain.create(ValueSet.ofRanges(Range.greaterThan(createVarcharType(columnPrecision), value1)), false))); // Shows what predicate is pushed down for the above query
                            assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                            return true;
                        }));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > null OR " + columnName + " < '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > null OR " + columnName + " < '" + value1 + "'"))
                .isNotFullyPushedDown(node(TableScanNode.class)
                        .with(TableScanNode.class, tableScanNode -> {
                            TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                            TupleDomain<?> expectedPredicate =
                                    TupleDomain.withColumnDomains(
                                            Map.of(
                                                    createColumnHandle(columnName, createVarcharType(columnPrecision), 12, jdbcTypeName, Optional.empty(), Optional.empty()),
                                                    Domain.create(ValueSet.ofRanges(Range.lessThan(createVarcharType(columnPrecision), value1)), false))); // Shows what predicate is pushed down for the above query
                            assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                            return true;
                        }));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= '" + value1 + "' OR " + columnName + " <= '" + value2 + "'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE NOT (" + columnName + " >= '" + value1 + "' OR " + columnName + " <= '" + value2 + "')"))
                .isReplacedWithEmptyValues();

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " > '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " >= '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " < '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " <= '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " = '" + value1 + "'"))
                .isFullyPushedDown();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " = null"))
                .isReplacedWithEmptyValues();
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " != '" + value1 + "'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " != '" + value1 + "'"))
                .isNotFullyPushedDown(node(TableScanNode.class)
                        .with(TableScanNode.class, tableScanNode -> {
                            TupleDomain<?> effectivePredicate = ((JdbcTableHandle) tableScanNode.getTable().getConnectorHandle()).getConstraint();
                            TupleDomain<?> expectedPredicate =
                                    TupleDomain.withColumnDomains(
                                            Map.of(
                                                    createColumnHandle(columnName, createVarcharType(columnPrecision), 12, jdbcTypeName, Optional.empty(), Optional.empty()),
                                                    Domain.create(ValueSet.ofRanges(Range.lessThan(createVarcharType(columnPrecision), Slices.utf8Slice(value1)), Range.greaterThan(createVarcharType(columnPrecision), Slices.utf8Slice(value1))), false))); // Shows what predicate is pushed down for the above query
                            assertThat(effectivePredicate).isEqualTo(expectedPredicate);
                            return true;
                        }));
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " != null"))
                .isReplacedWithEmptyValues();

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IS NULL"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IS NOT NULL"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " BETWEEN '" + value1 + "' AND '" + value2 + "'"))
                .isFullyPushedDown();

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " LIKE '%" + value1.charAt(0) + "%'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " LIKE '" + value1.charAt(0) + "%'"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " LIKE '" + value1.charAt(0) + "%" + value1.charAt(0) + "'"))
                .isNotFullyPushedDown(FilterNode.class);

        // Test different values lesser than or equals to `DEFAULT_COMPACTION_THRESHOLD`
        for (int count : largeInValuesCountData()) {
            String longStringValues = range(0, count)
                    .mapToObj("'a%s'"::formatted)
                    .collect(joining(", "));
            String longStringValuesMinusOne = range(0, count - 1) // Used for NOT IN clause as it generates (NOT IN parameters + 1) ranges which exceeds the `DEFAULT_COMPACTION_THRESHOLD` when tested with value equals to `DEFAULT_COMPACTION_THRESHOLD`
                    .mapToObj("'a%s'"::formatted)
                    .collect(joining(", "));

            assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IN (" + longStringValues + ")"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " NOT IN (" + longStringValuesMinusOne + ")"))
                    .isFullyPushedDown();

            assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " NOT IN (concat('a', " + columnName + "), " + longStringValues + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
            assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IN (concat('a', " + columnName + "), " + longStringValues + ")"))
                    .isNotFullyPushedDown(FilterNode.class);
        }

        // Test with (`DEFAULT_COMPACTION_THRESHOLD` + 1) to confirm that predicates are not pushed down
        String crossesThresholdStringValues = range(0, DEFAULT_COMPACTION_THRESHOLD + 1)
                .mapToObj("'a%s'"::formatted)
                .collect(joining(", "));
        String thresholdStringValues = range(0, DEFAULT_COMPACTION_THRESHOLD) // Used for NOT IN clause as it generates (NOT IN parameters + 1) ranges which exceeds the `DEFAULT_COMPACTION_THRESHOLD`
                .mapToObj("'a%s'"::formatted)
                .collect(joining(", "));

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IN (" + crossesThresholdStringValues + ")"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " NOT IN (" + thresholdStringValues + ")"))
                .isNotFullyPushedDown(FilterNode.class);

        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " NOT IN (concat('a', " + columnName + "), " + thresholdStringValues + ")"))
                .isNotFullyPushedDown(FilterNode.class);
        assertThat(query("SELECT * FROM " + tableName + " WHERE " + columnName + " IN (concat('a', " + columnName + "), " + crossesThresholdStringValues + ")"))
                .isNotFullyPushedDown(FilterNode.class);
    }

    private static JdbcColumnHandle createColumnHandle (String columnName, Type type, int jdbcType, String jdbcTypeName, Optional<Integer> columnSize, Optional<Integer> decimalDigits) {
        return new JdbcColumnHandle.Builder()
                .setColumnName(columnName)
                .setJdbcTypeHandle(
                        new JdbcTypeHandle(
                                jdbcType,
                                Optional.of(jdbcTypeName),
                                columnSize,
                                decimalDigits,
                                Optional.empty(),
                                Optional.empty()))
                .setComment(Optional.of("Dynamic Column."))
                .setColumnType(type)
                .setNullable(true)
                .build();
    }

    private List<Integer> largeInValuesCountData()
    {
        return ImmutableList.of(5, 17, DEFAULT_COMPACTION_THRESHOLD);
    }
}
