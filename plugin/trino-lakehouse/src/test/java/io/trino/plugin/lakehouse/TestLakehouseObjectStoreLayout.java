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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;

final class TestLakehouseObjectStoreLayout
        extends AbstractTestQueryFramework
{
    private static final String DELTA_LAYOUT_ENABLED_CATALOG = "lakehouse_delta_layout_enabled";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Floci floci = closeAfterClass(new Floci());
        floci.start();
        floci.createBucket("test-bucket");

        Map<String, String> commonProperties = ImmutableMap.of(
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", "s3://test-bucket/",
                "fs.s3.enabled", "true",
                "s3.endpoint", floci.endpoint().toString(),
                "s3.region", FLOCI_REGION,
                "s3.aws-access-key", FLOCI_ACCESS_KEY,
                "s3.aws-secret-key", FLOCI_SECRET_KEY,
                "s3.path-style-access", "true");
        QueryRunner queryRunner = LakehouseQueryRunner.builder()
                .setWorkerCount(0)
                .addLakehouseProperties(commonProperties)
                .addLakehouseProperty("iceberg.object-store-layout.enabled", "true")
                .addLakehouseProperty("delta.object-store-layout.enabled", "false")
                .build();
        queryRunner.createCatalog(DELTA_LAYOUT_ENABLED_CATALOG, "lakehouse", ImmutableMap.<String, String>builder()
                .putAll(commonProperties)
                .put("iceberg.object-store-layout.enabled", "false")
                .put("delta.object-store-layout.enabled", "true")
                .buildOrThrow());
        queryRunner.execute("CREATE SCHEMA lakehouse.tpch WITH (location = 's3://test-bucket/tpch')");
        return queryRunner;
    }

    @Test
    void testResetIcebergObjectStoreLayoutToDefault()
    {
        assertResetObjectStoreLayoutToDefault("lakehouse", "ICEBERG", "write.object-storage.enabled", true);
        assertResetObjectStoreLayoutToDefault(DELTA_LAYOUT_ENABLED_CATALOG, "ICEBERG", "write.object-storage.enabled", false);
    }

    @Test
    void testResetDeltaObjectStoreLayoutToDefault()
    {
        assertResetObjectStoreLayoutToDefault("lakehouse", "DELTA", "delta.randomizeFilePrefixes", false);
        assertResetObjectStoreLayoutToDefault(DELTA_LAYOUT_ENABLED_CATALOG, "DELTA", "delta.randomizeFilePrefixes", true);
    }

    private void assertResetObjectStoreLayoutToDefault(String catalog, String tableType, String storedProperty, boolean defaultValue)
    {
        Session session = Session.builder(getSession())
                .setCatalog(catalog)
                .build();
        try (TestTable table = new TestTable(
                sql -> getQueryRunner().execute(session, sql),
                "test_reset_object_store_layout_",
                "(value integer) WITH (type = '%s', object_store_layout_enabled = %s)".formatted(tableType, !defaultValue))) {
            String propertiesQuery = "SELECT coalesce(max(value), 'false') FROM \"%s$properties\" WHERE key = '%s'".formatted(table.getName(), storedProperty);
            assertQuery(session, propertiesQuery, "VALUES '%s'".formatted(!defaultValue));

            assertUpdate(session, "ALTER TABLE " + table.getName() + " SET PROPERTIES object_store_layout_enabled = DEFAULT");
            assertQuery(session, propertiesQuery, "VALUES '%s'".formatted(defaultValue));
        }
    }
}
