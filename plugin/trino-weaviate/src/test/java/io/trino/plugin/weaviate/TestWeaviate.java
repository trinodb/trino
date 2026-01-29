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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.GeoCoordinates;
import io.weaviate.client6.v1.api.collections.MultiTenancy;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.VectorConfig;
import io.weaviate.client6.v1.api.collections.Vectors;
import io.weaviate.client6.v1.api.collections.tenants.Tenant;
import io.weaviate.client6.v1.api.collections.vectorindex.Hnsw;
import io.weaviate.client6.v1.api.collections.vectorindex.MultiVector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestWeaviate
        extends BaseConnectorTest
{
    private static final String LARGE_COLLECTION = "TrinoTestLargs";
    private static final String COLLECTION_NO_TENANT = "TrinoTestNoTenant";
    private static final String COLLECTION_MULTI_TENANT = "TrinoCollectionMultiTenant";
    private static final String JOHN_DOE = "john_doe";
    private static final String JANE_DOE = "jane_doe";

    static final OffsetDateTime NOW = OffsetDateTime.now();
    static final GeoCoordinates GEO = new GeoCoordinates(123f, 456f);

    private WeaviateServer server;
    private WeaviateClient client;

    public TestWeaviate()
            throws IOException
    {
        server = new WeaviateServer();
        client = server.getClient();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HostAndPort httpAddress = server.getHttpAddress();
        HostAndPort grpcAddress = server.getGrpcAddress();
        return WeaviateQueryRunner.builder().addConnectorProperty("scheme", "http").addConnectorProperty("http-host", "localhost")
                .addConnectorProperty("http-port", String.valueOf(httpAddress.getPort()))
                .addConnectorProperty("grpc-host", "localhost")
                .addConnectorProperty("grpc-port", String.valueOf(grpcAddress.getPort()))
                .addConnectorProperty("consistency-level", "ONE").build();
    }

    @BeforeAll
    public final void setup()
            throws Exception
    {
        createTestCollection(LARGE_COLLECTION, 100);
        createTestCollection(COLLECTION_NO_TENANT, 1);
        createTestCollection(COLLECTION_MULTI_TENANT, 1, JOHN_DOE, JANE_DOE);
    }

    @AfterAll
    public final void destroy()
            throws Exception
    {
        server.close();
        server = null;
        client.close();
        client = null;
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM " + LARGE_COLLECTION, "SELECT 100");
    }

    @Test
    public void testSelectWithLimit()
    {
        assertQuery("SELECT prop_text FROM " + LARGE_COLLECTION + " LIMIT 1", "SELECT 'text-value'");
    }

    @Test
    public void testSelectVectorColumns()
    {
        assertQuery("SELECT vectors.single FROM " + COLLECTION_NO_TENANT, "SELECT CAST(ARRAY[1.0, 2.0, 3.0] AS DOUBLE ARRAY)");
        assertQuery("SELECT vectors.multi[1] FROM " + COLLECTION_NO_TENANT, "SELECT CAST(ARRAY[1.0, 2.0, 3.0] AS DOUBLE ARRAY)");
        assertQuery("SELECT vectors.multi[2] FROM " + COLLECTION_NO_TENANT, "SELECT CAST(ARRAY[4.0, 5.0, 6.0] AS DOUBLE ARRAY)");
    }

    @Test
    public void testSelectDeeplyNested()
    {
        assertQuery("SELECT prop_object.nested_object.deep_text FROM " + COLLECTION_NO_TENANT, "SELECT 'deep-text-value'");
        assertQuery("SELECT prop_object_array[1].array_int FROM " + COLLECTION_NO_TENANT, "SELECT 5");
    }

    @Test
    public void testSelectWithTenant()
    {
        assertQuery("SELECT prop_text, prop_bool FROM " + COLLECTION_NO_TENANT, "SELECT * FROM (VALUES ('text-value', true))");
        assertQuery("SELECT _vectors.single FROM " + COLLECTION_NO_TENANT, "SELECT CAST(ARRAY[1.0, 2.0, 3.0] as DOUBLE PRECISION ARRAY)");

        assertQuery("SELECT prop_text, prop_bool FROM " + withTenant(JOHN_DOE, COLLECTION_MULTI_TENANT), "SELECT * FROM (VALUES ('text-value', true))");
    }

    private static String withTenant(String tenant, String collectionName)
    {
        return tenant + "." + collectionName;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    private void createTestCollection(String collectionName, int size, String... tenants)
            throws IOException
    {
        boolean enableMultiTenancy = tenants.length > 0;
        boolean autoTenantCreation = enableMultiTenancy;
        boolean autoTenantActivation = enableMultiTenancy;
        CollectionHandle<Map<String, Object>> trinoTest = client.collections.create(
                collectionName, c -> c
                        .properties(
                                Property.text("prop_text"),
                                Property.bool("prop_bool"),
                                Property.integer("prop_int"),
                                Property.number("prop_number"),
                                Property.date("prop_date"),
                                Property.geoCoordinates("prop_geo_coordinates"),
                                Property.object(
                                        "prop_object",
                                        p -> p.nestedProperties(
                                                Property.text("prop_text"),
                                                Property.bool("prop_bool"),
                                                Property.integer("prop_int"),
                                                Property.number("prop_number"),
                                                Property.date("prop_date"),
                                                Property.object(
                                                        "nested_object",
                                                        n -> n.nestedProperties(
                                                                Property.text("deep_text"),
                                                                Property.integer("deep_int"))))),
                                Property.textArray("prop_text_array"),
                                Property.boolArray("prop_bool_array"),
                                Property.integerArray("prop_int_array"),
                                Property.numberArray("prop_number_array"),
                                Property.dateArray("prop_date_array"),
                                Property.objectArray(
                                        "prop_object_array",
                                        p -> p.nestedProperties(
                                                Property.text("array_text"),
                                                Property.integer("array_int"))))
                        .vectorConfig(
                                VectorConfig.selfProvided("single"),
                                VectorConfig.selfProvided("multi", multi -> multi
                                        .vectorIndex(Hnsw.of(hnsw -> hnsw
                                                .multiVector(MultiVector.of(
                                                        mv -> mv.enabled(true)))))))
                        .multiTenancy(MultiTenancy.of(mt -> mt
                                .enabled(enableMultiTenancy)
                                .autoTenantCreation(autoTenantCreation)
                                .autoTenantActivation(autoTenantActivation))));

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put("prop_text", "text-value");
        builder.put("prop_bool", true);
        builder.put("prop_int", 1L);
        builder.put("prop_number", 1D);
        builder.put("prop_date", NOW);
        builder.put("prop_geo_coordinates", GEO);
        builder.put("prop_object", Map.of(
                "prop_text", "text-value",
                "prop_bool", true,
                "prop_int", 1L,
                "prop_number", 1D,
                "prop_date", NOW,
                "nested_object", Map.of(
                        "deep_text", "deep-text-value",
                        "deep_int", -1L)));
        builder.put("prop_text_array", new String[] {"a", "b", "c"});
        builder.put("prop_bool_array", new boolean[] {true, true, false});
        builder.put("prop_int_array", new long[] {1L, 2L, 3L});
        builder.put("prop_number_array", new double[] {1D, 2D, 3D});
        builder.put("prop_date_array", new OffsetDateTime[] {NOW, NOW, NOW});
        builder.put("prop_object_array", new Map[] {
                Map.of(
                        "array_text", "array-text-value",
                        "array_int", 5L),
                Map.of(
                        "array_text", "array-text-value",
                        "array_int", 5L),
        });

        Map<String, Object> data = builder.buildOrThrow();
        Vectors vectors = new Vectors(
                Vectors.of("single", new float[] {1, 2, 3}),
                Vectors.of("multi", new float[][] {{1, 2, 3}, {4, 5, 6}}));

        if (tenants.length == 0) {
            for (var i = 0; i < size; i++) {
                trinoTest.data.insert(data, obj -> obj.vectors(vectors));
            }

            assertThat(trinoTest.size()).describedAs("inserted objects").isEqualTo(size);
            return;
        }

        for (var tenant : tenants) {
            CollectionHandle<Map<String, Object>> tenantHandle = trinoTest.withTenant(tenant);
            for (var i = 0; i < size; i++) {
                tenantHandle.data.insert(data, obj -> obj.vectors(vectors));
            }
            assertThat(tenantHandle.size()).describedAs("inserted objects").isEqualTo(size);
            assertThat(tenantHandle.tenants.list())
                    .describedAs("tenant %s exists", tenant)
                    .extracting(Tenant::name).contains(tenant);
        }
    }
}
