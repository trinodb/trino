/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamodb.testing;

import com.google.common.collect.ImmutableList;
import io.trino.testing.sql.TemporaryRelation;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DynamoDbTemporaryTable
        implements TemporaryRelation
{
    private final String tableName;
    private final DynamoDbClient dynamoDbClient;

    public DynamoDbTemporaryTable(
            String endpointUrl,
            String tableName,
            String keyName,
            KeyType keyType,
            ScalarAttributeType keyDataType,
            Optional<String> sortKeyName,
            Optional<KeyType> sortKeyType,
            Optional<ScalarAttributeType> sortKeyDataType,
            List<Map<String, AttributeValue>> rows)
    {
        dynamoDbClient =
                DynamoDbClient.builder()
                        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("accesskey", "secretkey")))
                        .region(Region.of("us-east-2"))
                        .endpointOverride(URI.create(endpointUrl))
                        .build();
        this.tableName = tableName;
        createTable(keyName, keyType, keyDataType, sortKeyName, sortKeyType, sortKeyDataType);

        rows.forEach(row ->
                dynamoDbClient.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(row).build()));
    }

    @Override
    public String getName()
    {
        return tableName;
    }

    @Override
    public void close()
    {
        dynamoDbClient.deleteTable(DeleteTableRequest.builder()
                .tableName(getName())
                .build());
        dynamoDbClient.close();
    }

    private void createTable(
            String keyName,
            KeyType keyType,
            ScalarAttributeType keyDataType,
            Optional<String> sortKeyName,
            Optional<KeyType> sortKeyType,
            Optional<ScalarAttributeType> sortKeyDataType)
    {
        ImmutableList.Builder<KeySchemaElement> keySchema = ImmutableList.builder();
        keySchema.add(KeySchemaElement.builder().attributeName(keyName).keyType(keyType).build());

        ImmutableList.Builder<AttributeDefinition> attributeDefinitions = ImmutableList.builder();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName(keyName).attributeType(keyDataType).build());

        if (sortKeyName.isPresent() && sortKeyType.isPresent() && sortKeyDataType.isPresent()) {
            keySchema.add(KeySchemaElement.builder().attributeName(sortKeyName.orElseThrow()).keyType(sortKeyType.orElseThrow()).build());
            attributeDefinitions.add(AttributeDefinition.builder().attributeName(sortKeyName.orElseThrow()).attributeType(sortKeyDataType.orElseThrow()).build());
        }
        else if (sortKeyName.isPresent() || sortKeyType.isPresent() || sortKeyDataType.isPresent()) {
            throw new IllegalArgumentException("sortKeyName, sortKeyType and sortKeyType are either be omitted or required");
        }

        CreateTableRequest.Builder builder = CreateTableRequest.builder()
                .attributeDefinitions(attributeDefinitions.build())
                .keySchema(keySchema.build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(1L)
                        .writeCapacityUnits(1L)
                        .build())
                .tableName(tableName);
        CreateTableRequest request = builder.attributeDefinitions(attributeDefinitions.build()).keySchema(keySchema.build()).build();

        dynamoDbClient.createTable(request);

        DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();
        WaiterResponse<DescribeTableResponse> waiterResponse = dynamoDbClient.waiter().waitUntilTableExists(tableRequest);
        if (waiterResponse.matched().response().isEmpty()) {
            throw new RuntimeException("Failed to create table " + tableName);
        }
    }
}
