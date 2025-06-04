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
package io.trino.plugin.cassandra;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonCassandraHandles
{
    private static final Map<String, Object> TABLE_HANDLE_AS_MAP = ImmutableMap.of(
            "relationHandle", ImmutableMap.of(
                    "@type", "named",
                    "schemaName", "cassandra_schema",
                    "tableName", "cassandra_table",
                    "clusteringKeyPredicates", ""));

    private static final Map<String, Object> TABLE2_HANDLE_AS_MAP = ImmutableMap.of(
            "relationHandle", ImmutableMap.of(
                    "@type", "named",
                    "schemaName", "cassandra_schema",
                    "tableName", "cassandra_table",
                    "partitions", List.of(
                            ImmutableMap.of(
                                    "key", "a2V5",
                                    "partitionId", "partitionKey1 = 11 AND partitionKey2 = 22",
                                    "tupleDomain", ImmutableMap.of("columnDomains", Collections.emptyList()),
                                    "indexedColumnPredicatePushdown", true)),
                    "clusteringKeyPredicates", "clusteringKey1 = 33"));

    private static final Map<String, Object> COLUMN_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("name", "column")
            .put("ordinalPosition", 42)
            .put("cassandraType", ImmutableMap.of(
                    "kind", "BIGINT",
                    "trinoType", "bigint",
                    "argumentTypes", ImmutableList.of()))
            .put("partitionKey", false)
            .put("clusteringKey", true)
            .put("indexed", false)
            .put("hidden", false)
            .buildOrThrow();

    private static final Map<String, Object> COLUMN2_HANDLE_AS_MAP = ImmutableMap.<String, Object>builder()
            .put("name", "column2")
            .put("ordinalPosition", 0)
            .put("cassandraType", ImmutableMap.of(
                    "kind", "SET",
                    "trinoType", "varchar",
                    "argumentTypes", ImmutableList.of()))
            .put("partitionKey", false)
            .put("clusteringKey", false)
            .put("indexed", false)
            .put("hidden", false)
            .buildOrThrow();

    private static final Optional<List<CassandraPartition>> PARTITIONS = Optional.of(List.of(
            new CassandraPartition(
                    "key".getBytes(UTF_8),
                    "partitionKey1 = 11 AND partitionKey2 = 22",
                    TupleDomain.all(),
                    true)));

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new CassandraClientModule.TypeDeserializer(TESTING_TYPE_MANAGER)));
        OBJECT_MAPPER = objectMapperProvider.get();
    }

    @Test
    public void testTableHandleSerialize()
            throws Exception
    {
        CassandraTableHandle tableHandle = new CassandraTableHandle(new CassandraNamedRelationHandle("cassandra_schema", "cassandra_table"));
        String json = OBJECT_MAPPER.writeValueAsString(tableHandle);
        testJsonEquals(json, TABLE_HANDLE_AS_MAP);
    }

    @Test
    public void testTable2HandleSerialize()
            throws Exception
    {
        CassandraTableHandle tableHandle = new CassandraTableHandle(new CassandraNamedRelationHandle("cassandra_schema", "cassandra_table", PARTITIONS, "clusteringKey1 = 33"));
        String json = OBJECT_MAPPER.writeValueAsString(tableHandle);
        testJsonEquals(json, TABLE2_HANDLE_AS_MAP);
    }

    @Test
    public void testTableHandleDeserialize()
            throws Exception
    {
        String json = OBJECT_MAPPER.writeValueAsString(TABLE_HANDLE_AS_MAP);

        CassandraNamedRelationHandle tableHandle = OBJECT_MAPPER.readValue(json, CassandraTableHandle.class).getRequiredNamedRelation();

        assertThat(tableHandle.getSchemaName()).isEqualTo("cassandra_schema");
        assertThat(tableHandle.getTableName()).isEqualTo("cassandra_table");
        assertThat(tableHandle.getSchemaTableName()).isEqualTo(new SchemaTableName("cassandra_schema", "cassandra_table"));
        assertThat(tableHandle.getClusteringKeyPredicates()).isEqualTo("");
    }

    @Test
    public void testTable2HandleDeserialize()
            throws Exception
    {
        String json = OBJECT_MAPPER.writeValueAsString(TABLE2_HANDLE_AS_MAP);

        CassandraNamedRelationHandle tableHandle = OBJECT_MAPPER.readValue(json, CassandraTableHandle.class).getRequiredNamedRelation();

        assertThat(tableHandle.getSchemaName()).isEqualTo("cassandra_schema");
        assertThat(tableHandle.getTableName()).isEqualTo("cassandra_table");
        assertThat(tableHandle.getSchemaTableName()).isEqualTo(new SchemaTableName("cassandra_schema", "cassandra_table"));
        assertThat(tableHandle.getPartitions()).isEqualTo(PARTITIONS);
        assertThat(tableHandle.getClusteringKeyPredicates()).isEqualTo("clusteringKey1 = 33");
    }

    @Test
    public void testColumnHandleSerialize()
            throws Exception
    {
        CassandraColumnHandle columnHandle = new CassandraColumnHandle("column", 42, CassandraTypes.BIGINT, false, true, false, false);
        String json = OBJECT_MAPPER.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN_HANDLE_AS_MAP);
    }

    @Test
    public void testColumn2HandleSerialize()
            throws Exception
    {
        CassandraColumnHandle columnHandle = new CassandraColumnHandle(
                "column2",
                0,
                CassandraTypes.SET,
                false,
                false,
                false,
                false);
        String json = OBJECT_MAPPER.writeValueAsString(columnHandle);
        testJsonEquals(json, COLUMN2_HANDLE_AS_MAP);
    }

    @Test
    public void testColumnHandleDeserialize()
            throws Exception
    {
        String json = OBJECT_MAPPER.writeValueAsString(COLUMN_HANDLE_AS_MAP);

        CassandraColumnHandle columnHandle = OBJECT_MAPPER.readValue(json, CassandraColumnHandle.class);

        assertThat(columnHandle.name()).isEqualTo("column");
        assertThat(columnHandle.ordinalPosition()).isEqualTo(42);
        assertThat(columnHandle.cassandraType()).isEqualTo(CassandraTypes.BIGINT);
        assertThat(columnHandle.partitionKey()).isEqualTo(false);
        assertThat(columnHandle.clusteringKey()).isEqualTo(true);
    }

    @Test
    public void testColumn2HandleDeserialize()
            throws Exception
    {
        String json = OBJECT_MAPPER.writeValueAsString(COLUMN2_HANDLE_AS_MAP);

        CassandraColumnHandle columnHandle = OBJECT_MAPPER.readValue(json, CassandraColumnHandle.class);

        assertThat(columnHandle.name()).isEqualTo("column2");
        assertThat(columnHandle.ordinalPosition()).isEqualTo(0);
        assertThat(columnHandle.cassandraType()).isEqualTo(CassandraTypes.SET);
        assertThat(columnHandle.partitionKey()).isEqualTo(false);
        assertThat(columnHandle.clusteringKey()).isEqualTo(false);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
