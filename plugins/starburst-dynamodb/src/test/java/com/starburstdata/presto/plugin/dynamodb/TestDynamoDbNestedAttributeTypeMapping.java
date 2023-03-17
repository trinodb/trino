/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.io.File;
import java.net.URI;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbSessionProperties.FLATTEN_ARRAY_ELEMENT_COUNT;
import static com.starburstdata.presto.plugin.dynamodb.DynamoDbSessionProperties.FLATTEN_OBJECTS_ENABLED;

public class TestDynamoDbNestedAttributeTypeMapping
        extends AbstractTestQueryFramework
{
    private DynamoDbClient dynamoDbClient;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File schemaDirectory = Paths.get("src/test/resources/complex-type-schemas").toFile();
        checkState(schemaDirectory.exists() && schemaDirectory.isDirectory(), "Test schema directory " + schemaDirectory + " does not exist or is not a directory");

        TestingDynamoDbServer server = closeAfterClass(new TestingDynamoDbServer());

        DynamoDbClientBuilder builder = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key")))
                .region(Region.of("us-east-2"))
                .endpointOverride(URI.create(server.getEndpointUrl()));

        dynamoDbClient = closeAfterClass(builder.build());

        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), schemaDirectory)
                // copy smallest table to enforce DynamicCatalogManager load catalogs
                .setTables(ImmutableList.of(TpchTable.REGION))
                .enableWrites()
                .build();
    }

    @Test
    public void testStringSet()
    {
        String tableName = "test_string_set";
        prepareStringSetData(tableName);
        assertQuery(
                "SELECT row_id, value FROM " + tableName,
                "SELECT 'a' AS row_id, '[\"a\"]' AS value UNION " +
                        "SELECT 'b' AS row_id, '[\"a\",\"b\"]' AS value UNION " +
                        "SELECT 'c' AS row_id, '[\"a\",\"b\",\"c\"]' AS value");
    }

    @Test
    public void testFlattenedStringSet()
    {
        String tableName = "test_flattened_string_set";
        prepareStringSetData(tableName);
        assertQuery(Session.builder(getSession())
                        .setCatalogSessionProperty("dynamodb", FLATTEN_ARRAY_ELEMENT_COUNT, "3")
                        .build(),
                "SELECT row_id, \"value.0\", \"value.1\", \"value.2\" FROM " + tableName,
                "SELECT 'a' AS row_id, 'a' AS \"value.0\", NULL AS \"value.1\", NULL AS \"value.2\" UNION " +
                        "SELECT 'b' AS row_id, 'a' AS \"value.0\", 'b' AS \"value.1\", NULL AS \"value.2\" UNION " +
                        "SELECT 'c' AS row_id, 'a' AS \"value.0\", 'b' AS \"value.1\", 'c' AS \"value.2\"");
    }

    @Test
    public void testMap()
    {
        String tableName = "test_map";
        prepareMapData(tableName);
        assertQuery(
                "SELECT row_id, value FROM " + tableName,
                "SELECT 'a' AS row_id, '{\"a\":\"abc\"}' AS value UNION " +
                        "SELECT 'b' AS row_id, '{\"a\":\"abc\",\"b\":123}' AS value UNION " +
                        "SELECT 'c' AS row_id, '{\"a\":\"abc\",\"b\":123,\"c\":[\"def\",\"ghi\"]}' AS value");
    }

    @Test
    public void testFlattenedMap()
    {
        String tableName = "test_flattened_map";
        prepareMapData(tableName);
        assertQuery(Session.builder(getSession())
                        .setCatalogSessionProperty("dynamodb", FLATTEN_OBJECTS_ENABLED, "true")
                        .build(),
                "SELECT row_id, \"value.a\", \"value.b\", \"value.c\" FROM " + tableName,
                "SELECT 'a' AS row_id, 'abc' AS \"value.a\", NULL AS \"value.b\", NULL AS \"value.c.0\" UNION " +
                        "SELECT 'b' AS row_id, 'abc' AS \"value.a\", 123 AS \"value.b\", NULL AS \"value.c.0\" UNION " +
                        "SELECT 'c' AS row_id, 'abc' AS \"value.a\", 123 AS \"value.b\", '[\"def\",\"ghi\"]' AS \"value.c.0\"");
    }

    @Test
    public void testFlattenedMapFlattenedArrays()
    {
        String tableName = "test_flattened_map_flattened_arrays";
        prepareMapData(tableName);
        assertQuery(Session.builder(getSession())
                        .setCatalogSessionProperty("dynamodb", FLATTEN_OBJECTS_ENABLED, "true")
                        .setCatalogSessionProperty("dynamodb", FLATTEN_ARRAY_ELEMENT_COUNT, "2")
                        .build(),
                "SELECT row_id, \"value.a\", \"value.b\", \"value.c.0\", \"value.c.1\" FROM " + tableName,
                "SELECT 'a' AS row_id, 'abc' AS \"value.a\", NULL AS \"value.b\", NULL AS \"value.c.0\", NULL AS \"value.c.1\" UNION " +
                        "SELECT 'b' AS row_id, 'abc' AS \"value.a\", 123 AS \"value.b\", NULL AS \"value.c.0\", NULL AS \"value.c.1\" UNION " +
                        "SELECT 'c' AS row_id, 'abc' AS \"value.a\", 123 AS \"value.b\", 'def' AS \"value.c.0\", 'ghi' AS \"value.c.1\"");
    }

    private void createTable(String tableName)
    {
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("row_id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName("row_id")
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(1L)
                        .writeCapacityUnits(1L)
                        .build())
                .tableName(tableName)
                .build();

        dynamoDbClient.createTable(request);

        DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();
        WaiterResponse<DescribeTableResponse> waiterResponse = dynamoDbClient.waiter().waitUntilTableExists(tableRequest);
        if (waiterResponse.matched().response().isEmpty()) {
            throw new RuntimeException("Failed to create table " + tableName);
        }
    }

    private void prepareStringSetData(String tableName)
    {
        createTable(tableName);

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(ImmutableMap.of(
                        "row_id", AttributeValue.builder().s("a").build(),
                        "value", AttributeValue.builder().ss("a").build()
                )).build());

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(ImmutableMap.of(
                        "row_id", AttributeValue.builder().s("b").build(),
                        "value", AttributeValue.builder().ss("a", "b").build()
                )).build());

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(ImmutableMap.of(
                        "row_id", AttributeValue.builder().s("c").build(),
                        "value", AttributeValue.builder().ss("a", "b", "c").build()
                )).build());
    }

    private void prepareMapData(String tableName)
    {
        createTable(tableName);

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(ImmutableMap.of(
                        "row_id", AttributeValue.builder().s("a").build(),
                        "value", AttributeValue.builder().m(ImmutableMap.of("a", AttributeValue.builder().s("abc").build())).build()
                )).build());

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(ImmutableMap.of(
                        "row_id", AttributeValue.builder().s("b").build(),
                        "value", AttributeValue.builder().m(ImmutableMap.of(
                                "a", AttributeValue.builder().s("abc").build(),
                                "b", AttributeValue.builder().n("123").build()
                        )).build()
                )).build());

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(ImmutableMap.of(
                        "row_id", AttributeValue.builder().s("c").build(),
                        "value", AttributeValue.builder().m(ImmutableMap.of(
                                "a", AttributeValue.builder().s("abc").build(),
                                "b", AttributeValue.builder().n("123").build(),
                                "c", AttributeValue.builder().ss("def", "ghi").build()
                        )).build()
                )).build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        dynamoDbClient = null;
    }
}
