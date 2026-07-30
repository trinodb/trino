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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDynamoDbTypeMapping
        extends AbstractTestQueryFramework
{
    private DynamoDbServer server;
    private DynamoDbClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = closeAfterClass(new DynamoDbServer());
        client = closeAfterClass(server.createClient());
        return DynamoDbQueryRunner.builder(server).build();
    }

    @BeforeAll
    public void createTestTables()
    {
        createTypeMappingTable();
        createBooleanTable();
        createTypeWideningTable();
    }

    @AfterAll
    public void dropTestTables()
    {
        client.deleteTable(r -> r.tableName("type_mapping"));
        client.deleteTable(r -> r.tableName("boolean_table"));
        client.deleteTable(r -> r.tableName("widened_table"));
    }

    @Test
    public void testNumberKeyMapsToDouble()
    {
        assertThat(query("SELECT pk FROM type_mapping WHERE pk = 1")).result()
                .hasTypes(List.of(DOUBLE));
    }

    @Test
    public void testStringAttributeMapsToVarchar()
    {
        assertThat(query("SELECT str_val FROM type_mapping WHERE pk = 1")).result()
                .hasTypes(List.of(VARCHAR));
    }

    @Test
    public void testNumberAttributeMapsToDouble()
    {
        assertThat(query("SELECT num_val FROM type_mapping WHERE pk = 1")).result()
                .hasTypes(List.of(DOUBLE));
    }

    @Test
    public void testBooleanAttributeMapsToBoolean()
    {
        assertThat(query("SELECT bool_val FROM boolean_table WHERE pk = 1")).result()
                .hasTypes(List.of(BOOLEAN));
    }

    @Test
    public void testNullAttributeIsNull()
    {
        assertQuery("SELECT pk FROM type_mapping WHERE str_val IS NULL", "VALUES CAST(2.0 AS DOUBLE)");
    }

    @Test
    public void testMapAttributeMapsToVarcharJson()
    {
        // DynamoDB M (map) type is serialized as a VARCHAR with key-value pairs.
        // String values inside maps are unquoted: {"key":value} rather than {"key":"value"}.
        assertThat(query("SELECT map_val FROM type_mapping WHERE pk = 1")).result()
                .hasTypes(List.of(VARCHAR));
        assertQuery("SELECT map_val FROM type_mapping WHERE pk = 1", "VALUES CAST('{\"key\":value}' AS VARCHAR)");
    }

    @Test
    public void testListAttributeMapsToVarcharJson()
    {
        // DynamoDB L (list) type is serialized as a VARCHAR JSON array.
        // Numeric list elements are serialized without quotes: [1,2,3].
        assertThat(query("SELECT list_val FROM type_mapping WHERE pk = 1")).result()
                .hasTypes(List.of(VARCHAR));
        assertQuery("SELECT list_val FROM type_mapping WHERE pk = 1", "VALUES CAST('[1,2,3]' AS VARCHAR)");
    }

    @Test
    public void testMixedTypesWidenToVarchar()
    {
        // When the same attribute has N in some items and S in others, it is widened to VARCHAR
        assertThat(query("SELECT mixed_val FROM widened_table")).result()
                .hasTypes(List.of(VARCHAR));
    }

    @Test
    public void testBooleanValues()
    {
        assertThat(query("SELECT pk, bool_val FROM boolean_table ORDER BY pk")).result()
                .matches(resultBuilder(getSession(), DOUBLE, BOOLEAN)
                        .row(1.0, true)
                        .row(2.0, false)
                        .build());
    }

    @Test
    public void testStringKeyMapsToVarchar()
    {
        // Create a table with S (string) hash key
        client.createTable(r -> r
                .tableName("string_key_table")
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));
        client.putItem(PutItemRequest.builder()
                .tableName("string_key_table")
                .item(Map.of("pk", AttributeValue.fromS("hello"), "val", AttributeValue.fromN("42")))
                .build());
        try {
            assertThat(query("SELECT pk FROM string_key_table")).result()
                    .hasTypes(List.of(VARCHAR));
            assertQuery("SELECT pk FROM string_key_table", "VALUES CAST('hello' AS VARCHAR)");
        }
        finally {
            client.deleteTable(r -> r.tableName("string_key_table"));
        }
    }

    private void createTypeMappingTable()
    {
        client.createTable(r -> r
                .tableName("type_mapping")
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.N).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));

        // Item 1: all attribute types present
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.fromN("1"));
        item1.put("str_val", AttributeValue.fromS("hello"));
        item1.put("num_val", AttributeValue.fromN("42.5"));
        item1.put("map_val", AttributeValue.fromM(Map.of("key", AttributeValue.fromS("value"))));
        item1.put("list_val", AttributeValue.fromL(List.of(
                AttributeValue.fromN("1"),
                AttributeValue.fromN("2"),
                AttributeValue.fromN("3"))));
        client.putItem(PutItemRequest.builder().tableName("type_mapping").item(item1).build());

        // Item 2: str_val is absent (null in Trino)
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.fromN("2"));
        item2.put("num_val", AttributeValue.fromN("99.0"));
        client.putItem(PutItemRequest.builder().tableName("type_mapping").item(item2).build());
    }

    private void createBooleanTable()
    {
        client.createTable(r -> r
                .tableName("boolean_table")
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.N).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));

        client.putItem(PutItemRequest.builder()
                .tableName("boolean_table")
                .item(Map.of("pk", AttributeValue.fromN("1"), "bool_val", AttributeValue.fromBool(true)))
                .build());
        client.putItem(PutItemRequest.builder()
                .tableName("boolean_table")
                .item(Map.of("pk", AttributeValue.fromN("2"), "bool_val", AttributeValue.fromBool(false)))
                .build());
    }

    private void createTypeWideningTable()
    {
        client.createTable(r -> r
                .tableName("widened_table")
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("pk").attributeType(ScalarAttributeType.N).build())
                .billingMode(BillingMode.PAY_PER_REQUEST));

        // mixed_val is N in one item and S in another → widened to VARCHAR
        client.putItem(PutItemRequest.builder()
                .tableName("widened_table")
                .item(Map.of("pk", AttributeValue.fromN("1"), "mixed_val", AttributeValue.fromN("123")))
                .build());
        client.putItem(PutItemRequest.builder()
                .tableName("widened_table")
                .item(Map.of("pk", AttributeValue.fromN("2"), "mixed_val", AttributeValue.fromS("text")))
                .build());
    }
}
