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
package io.trino.plugin.dynamodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.PreDestroy;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DynamoDbService
{
    static final String DEFAULT_SCHEMA = "default";

    private final DynamoDbClient dynamoDbClient;
    private final int scanSegments;

    @Inject
    public DynamoDbService(DynamoDbConfig config)
    {
        requireNonNull(config, "config is null");

        Optional<String> accessKey = config.getAwsAccessKey();
        Optional<String> secretKey = config.getAwsSecretKey();
        String region = config.getAwsRegion();
        this.scanSegments = config.getScanSegments();

        AwsCredentialsProvider baseCredentialsProvider;
        if (accessKey.isPresent() && secretKey.isPresent()) {
            baseCredentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey.get(), secretKey.get()));
        }
        else {
            baseCredentialsProvider = DefaultCredentialsProvider.create();
        }

        AwsCredentialsProvider credentialsProvider;
        if (config.getIamRole().isPresent()) {
            StsClient stsClient = StsClient.builder()
                    .region(Region.of(region))
                    .credentialsProvider(baseCredentialsProvider)
                    .build();
            credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(config.getIamRole().get())
                            .roleSessionName("trino-dynamodb")
                            .externalId(config.getExternalId().orElse(null)))
                    .stsClient(stsClient)
                    .asyncCredentialUpdateEnabled(true)
                    .build();
        }
        else {
            credentialsProvider = baseCredentialsProvider;
        }

        var clientBuilder = DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider);
        config.getEndpointOverride().ifPresent(clientBuilder::endpointOverride);
        this.dynamoDbClient = clientBuilder.build();
    }

    @PreDestroy
    public void close()
    {
        dynamoDbClient.close();
    }

    public int getScanSegments()
    {
        return scanSegments;
    }

    public List<String> listTableNames()
    {
        List<String> tableNames = new ArrayList<>();
        ListTablesResponse response = dynamoDbClient.listTables();
        tableNames.addAll(response.tableNames());

        while (response.lastEvaluatedTableName() != null) {
            response = dynamoDbClient.listTables(ListTablesRequest.builder()
                    .exclusiveStartTableName(response.lastEvaluatedTableName())
                    .build());
            tableNames.addAll(response.tableNames());
        }

        return ImmutableList.copyOf(tableNames);
    }

    public List<DynamoDbColumnHandle> getTableColumns(String tableName)
    {
        TableDescription table = dynamoDbClient.describeTable(
                DescribeTableRequest.builder().tableName(tableName).build())
                .table();

        Map<String, ScalarAttributeType> keyAttributeTypes = buildKeyAttributeTypeMap(table);
        List<String> keyOrder = buildKeyOrder(table);

        // sample up to 100 items to discover non-key attribute names and types
        ScanResponse sampleResponse = dynamoDbClient.scan(ScanRequest.builder()
                .tableName(tableName)
                .limit(100)
                .build());

        // track discovered attribute name → inferred Trino type, skipping keys (handled separately)
        Map<String, Type> discoveredAttributes = new LinkedHashMap<>();
        for (Map<String, AttributeValue> item : sampleResponse.items()) {
            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                String attributeName = entry.getKey();
                if (keyAttributeTypes.containsKey(attributeName)) {
                    continue;
                }
                Type inferredType = inferType(entry.getValue());
                discoveredAttributes.merge(attributeName, inferredType, DynamoDbService::widenType);
            }
        }

        ImmutableList.Builder<DynamoDbColumnHandle> columns = ImmutableList.builder();
        int ordinal = 0;

        // key attributes first, in key schema order (HASH then RANGE)
        for (String keyName : keyOrder) {
            Type type = attributeTypeToTrinoType(keyAttributeTypes.get(keyName));
            columns.add(new DynamoDbColumnHandle(keyName, type, ordinal));
            ordinal++;
        }

        // non-key attributes alphabetically
        List<String> nonKeyNames = new ArrayList<>(discoveredAttributes.keySet());
        nonKeyNames.sort(String::compareTo);
        for (String attributeName : nonKeyNames) {
            columns.add(new DynamoDbColumnHandle(attributeName, discoveredAttributes.get(attributeName), ordinal));
            ordinal++;
        }

        return columns.build();
    }

    public ScanIterable scanTable(String tableName, int segment, int totalSegments)
    {
        ScanRequest.Builder requestBuilder = ScanRequest.builder()
                .tableName(tableName);

        if (totalSegments > 1) {
            requestBuilder.segment(segment).totalSegments(totalSegments);
        }

        return dynamoDbClient.scanPaginator(requestBuilder.build());
    }

    private static Map<String, ScalarAttributeType> buildKeyAttributeTypeMap(TableDescription table)
    {
        ImmutableMap.Builder<String, ScalarAttributeType> map = ImmutableMap.builder();
        for (AttributeDefinition definition : table.attributeDefinitions()) {
            map.put(definition.attributeName(), definition.attributeType());
        }
        return map.buildOrThrow();
    }

    private static List<String> buildKeyOrder(TableDescription table)
    {
        List<String> hashKeys = new ArrayList<>();
        List<String> rangeKeys = new ArrayList<>();
        for (KeySchemaElement element : table.keySchema()) {
            if (element.keyType() == KeyType.HASH) {
                hashKeys.add(element.attributeName());
            }
            else {
                rangeKeys.add(element.attributeName());
            }
        }
        return ImmutableList.<String>builder().addAll(hashKeys).addAll(rangeKeys).build();
    }

    private static Type attributeTypeToTrinoType(ScalarAttributeType attributeType)
    {
        return switch (attributeType) {
            case N -> DoubleType.DOUBLE;
            case S, B, UNKNOWN_TO_SDK_VERSION -> VarcharType.VARCHAR;
        };
    }

    static Type inferType(AttributeValue value)
    {
        return switch (value.type()) {
            case S -> VarcharType.VARCHAR;
            case N -> DoubleType.DOUBLE;
            case BOOL -> BooleanType.BOOLEAN;
            case M, L, SS, NS, BS, B, NUL, UNKNOWN_TO_SDK_VERSION -> VarcharType.VARCHAR;
        };
    }

    // widening rule: if two observations differ, fall back to VARCHAR
    private static Type widenType(Type existing, Type incoming)
    {
        if (existing.equals(incoming)) {
            return existing;
        }
        return VarcharType.VARCHAR;
    }
}
