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

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.dynamodb.testing.DynamoDbTemporaryTable;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVarcharPrecision
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
                                "dynamodb.extra-jdbc-properties", "SchemaCacheDuration=0")) // Don't cache table metadata. Required for created test table to show up.
                .enablePredicatePushdown()
                .enableWrites()
                .build();
    }

    @Test
    public void testVarcharPrecision()
    {
        String primaryKey = "row_id";
        KeyType primaryType = KeyType.HASH;
        ScalarAttributeType primaryKeyDataType = ScalarAttributeType.S;

        String col1 = "col1";
        int col1RandomPrecision = 500;
        String col1RandomPrecisionValue = "a".repeat(col1RandomPrecision);
        int col1MaxPrecision = (400 * 1024) - 11; // Based on the 400KB max DynamoDB item size. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-items
        String col1MaxPrecisionValue = "a".repeat(col1MaxPrecision);

        try (DynamoDbTemporaryTable table
                = new DynamoDbTemporaryTable(
                        server.getEndpointUrl(),
                "test_varchar_precision" + randomNameSuffix(),
                        primaryKey,
                        primaryType,
                        primaryKeyDataType,
                        Optional.empty(),
                        Optional.empty(),
                Optional.empty(),
                        List.of(
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("a").build(),
                                        col1, AttributeValue.builder().s(col1RandomPrecisionValue).build()),
                                ImmutableMap.of(
                                        primaryKey, AttributeValue.builder().s("b").build(),
                                        col1, AttributeValue.builder().s(col1MaxPrecisionValue).build())))) {
            String tableName = table.getName();
            assertThat((String) computeActual(
                    "SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("""
                        CREATE TABLE dynamodb.amazondynamodb.%s (
                           %s varchar NOT NULL COMMENT 'Dynamic Column.',
                           %s varchar COMMENT 'Dynamic Column.'
                        )""".formatted(tableName, primaryKey, col1));

            assertQuery("SELECT row_id, col1 FROM " + tableName, "VALUES ('a', '%s'), ('b', '%s')".formatted(col1RandomPrecisionValue, col1MaxPrecisionValue));
        }
    }

    @Test
    public void testPrimarySortKeyVarcharPrecision()
    {
        String primaryKey = "row_id";
        KeyType primaryKeyType = KeyType.HASH;
        ScalarAttributeType primaryKeyDataType = ScalarAttributeType.S;

        String sortKey = "sort_key";
        KeyType sortKeyType = KeyType.RANGE;
        ScalarAttributeType sortKeyDataType = ScalarAttributeType.S;

        int primaryKeyMaxPrecision = 2048; // Based on the max partition key size. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-partition-sort-keys
        int sortKeyMaxPrecision = 1024; // Based on the max sort key size. https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-partition-sort-keys

        String col1 = "col1";
        int col1RandomPrecision = 500;
        String col1RandomPrecisionValue = "a".repeat(col1RandomPrecision);

        String sortKeyRow1 = "a".repeat(sortKeyMaxPrecision);
        String sortKey1Row2 = "b".repeat(sortKeyMaxPrecision);

        String primaryKeyRow1 = "a".repeat(primaryKeyMaxPrecision);
        String primaryKeyRow2 = "b".repeat(primaryKeyMaxPrecision);

        try (DynamoDbTemporaryTable table
                = new DynamoDbTemporaryTable(
                server.getEndpointUrl(),
                "test_varchar_precision" + randomNameSuffix(),
                primaryKey,
                primaryKeyType,
                primaryKeyDataType,
                Optional.of(sortKey),
                Optional.of(sortKeyType),
                Optional.of(sortKeyDataType),
                List.of(
                        ImmutableMap.of(
                                primaryKey, AttributeValue.builder().s(primaryKeyRow1).build(),
                                sortKey, AttributeValue.builder().s(sortKeyRow1).build(),
                                col1, AttributeValue.builder().s(col1RandomPrecisionValue).build()),
                        ImmutableMap.of(
                                primaryKey, AttributeValue.builder().s(primaryKeyRow2).build(),
                                sortKey, AttributeValue.builder().s(sortKey1Row2).build(),
                                col1, AttributeValue.builder().s(col1RandomPrecisionValue).build())))) {
            String tableName = table.getName();
            assertThat((String) computeActual(
                    "SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo("""
                        CREATE TABLE dynamodb.amazondynamodb.%s (
                           %s varchar NOT NULL COMMENT 'Dynamic Column.',
                           %s varchar NOT NULL COMMENT 'Dynamic Column.',
                           %s varchar COMMENT 'Dynamic Column.'
                        )""".formatted(tableName, sortKey, primaryKey, col1));

            assertQuery("SELECT %s, %s, %s FROM %s".formatted(primaryKey, sortKey, col1, tableName),
                    "VALUES ('%s', '%s', '%s'), ('%s', '%s', '%s')".formatted(primaryKeyRow1, sortKeyRow1, col1RandomPrecisionValue, primaryKeyRow2, sortKey1Row2, col1RandomPrecisionValue));
        }
    }
}
