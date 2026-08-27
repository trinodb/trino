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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Execution(ExecutionMode.SAME_THREAD)
public class TestPaimonMinioSmokeTest
        extends AbstractTestQueryFramework
{
    private static final String CATALOG = "paimon";
    private static final String SCHEMA = "minio_smoke";
    private static final String WAREHOUSE_PREFIX = "warehouse";

    private final String bucketName = "test-paimon-minio-" + randomNameSuffix();
    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        assumeLocalMinioProxyBypassed();
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);

        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();

        queryRunner.installPlugin(new PaimonPlugin());
        queryRunner.createCatalog(
                CATALOG,
                CATALOG,
                ImmutableMap.<String, String>builder()
                        .put("warehouse", "s3://%s/%s".formatted(bucketName, WAREHOUSE_PREFIX))
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("lock.enabled", "true")
                        .put("lock.type", "trino-test")
                        .put("fs.s3a.access.key", MINIO_ROOT_USER)
                        .put("fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
                        .put("fs.s3a.endpoint.region", MINIO_REGION)
                        .put("fs.s3a.endpoint", minio.getMinioAddress())
                        .put("fs.s3a.path.style.access", "true")
                        .buildOrThrow());

        return queryRunner;
    }

    private static void assumeLocalMinioProxyBypassed()
    {
        boolean proxyConfigured = isNonBlank(System.getenv("HTTP_PROXY"))
                || isNonBlank(System.getenv("HTTPS_PROXY"))
                || isNonBlank(System.getenv("http_proxy"))
                || isNonBlank(System.getenv("https_proxy"));
        if (!proxyConfigured) {
            return;
        }

        assumeTrue(noProxyCovers("localhost", System.getenv("NO_PROXY"), System.getenv("no_proxy"))
                        && noProxyCovers("127.0.0.1", System.getenv("NO_PROXY"), System.getenv("no_proxy")),
                "Local MinIO smoke requires NO_PROXY/no_proxy to include localhost,127.0.0.1 when HTTP proxy variables are set");
    }

    private static boolean noProxyCovers(String host, String... noProxyValues)
    {
        for (String noProxy : noProxyValues) {
            if (noProxyValueCovers(noProxy, host)) {
                return true;
            }
        }
        return false;
    }

    private static boolean noProxyValueCovers(String noProxy, String host)
    {
        if (!isNonBlank(noProxy)) {
            return false;
        }

        String normalizedHost = host.toLowerCase(Locale.ROOT);
        for (String entry : noProxy.split(",")) {
            String normalizedEntry = entry.trim().toLowerCase(Locale.ROOT);
            if (normalizedEntry.equals("*") || normalizedEntry.equals(normalizedHost)) {
                return true;
            }
            if (normalizedHost.equals("localhost") && normalizedEntry.equals(".localhost")) {
                return true;
            }
            if (normalizedHost.equals("127.0.0.1")
                    && (normalizedEntry.equals("127.0.0.0/8") || normalizedEntry.equals("127.*"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNonBlank(String value)
    {
        return value != null && !value.isBlank();
    }

    @Test
    public void testMinioWarehouseCrudDdlSmoke()
    {
        String tableName = "orders_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertThat(computeActual("SHOW SCHEMAS FROM " + CATALOG).getOnlyColumnAsSet()).contains(SCHEMA);

            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "orderkey bigint, "
                    + "status varchar COMMENT 'order status') "
                    + "COMMENT 'orders smoke table'");
            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES (1, 'ok'), (2, 'ready')", 2);

            assertQuery(
                    "SELECT * FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES (CAST(1 AS BIGINT), CAST('ok' AS VARCHAR)), (CAST(2 AS BIGINT), CAST('ready' AS VARCHAR))");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + qualifiedTableName))
                    .contains("COMMENT 'orders smoke table'")
                    .contains("COMMENT 'order status'");

            List<String> warehouseObjects = listObjects(
                    WAREHOUSE_PREFIX + "/" + SCHEMA + ".db/" + tableName);
            assertThat(warehouseObjects)
                    .isNotEmpty()
                    .anyMatch(path -> !path.endsWith("_trino_paimon_directory_marker"));
            assertThat(warehouseObjects).allMatch(path -> path.startsWith(WAREHOUSE_PREFIX + "/" + SCHEMA + ".db/" + tableName));

            assertUpdate("ALTER TABLE " + qualifiedTableName + " RENAME COLUMN status TO state");
            assertUpdate("COMMENT ON COLUMN " + qualifiedTableName + ".state IS 'current state'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + qualifiedTableName))
                    .contains("state varchar COMMENT 'current state'");
            assertQuery(
                    "SELECT orderkey, state FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES (CAST(1 AS BIGINT), CAST('ok' AS VARCHAR)), (CAST(2 AS BIGINT), CAST('ready' AS VARCHAR))");

            assertUpdate("DELETE FROM " + qualifiedTableName);
            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(0 AS BIGINT)");

            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES (3, 'shipped'), (4, 'closed')", 2);
            assertUpdate("TRUNCATE TABLE " + qualifiedTableName);
            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(0 AS BIGINT)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }

    @Test
    public void testMinioPartitionPredicateDeleteSmoke()
    {
        String tableName = "partition_delete_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "orderkey bigint, "
                    + "status varchar, "
                    + "ds varchar) "
                    + "WITH (partitioned_by = ARRAY['ds'])");
            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES "
                    + "(1, 'queued', '2026-07-01'), "
                    + "(2, 'ready', '2026-07-02'), "
                    + "(3, 'done', '2026-07-02'), "
                    + "(4, 'held', '2026-07-03')", 4);

            assertUpdate("DELETE FROM " + qualifiedTableName + " WHERE ds = '2026-07-01'");
            assertQuery(
                    "SELECT orderkey, status, ds FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES "
                            + "(CAST(2 AS BIGINT), CAST('ready' AS VARCHAR), CAST('2026-07-02' AS VARCHAR)), "
                            + "(CAST(3 AS BIGINT), CAST('done' AS VARCHAR), CAST('2026-07-02' AS VARCHAR)), "
                            + "(CAST(4 AS BIGINT), CAST('held' AS VARCHAR), CAST('2026-07-03' AS VARCHAR))");

            assertUpdate("DELETE FROM " + qualifiedTableName + " WHERE ds IN ('2026-07-02', '2026-07-03')");
            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(0 AS BIGINT)");

            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES "
                    + "(5, 'partial', '2026-07-04'), "
                    + "(6, 'keep', '2026-07-04')", 2);
            assertQueryFails(
                    "DELETE FROM " + qualifiedTableName + " WHERE ds = '2026-07-04' AND orderkey = 5",
                    ".*Paimon.*delete.*");
            assertQuery(
                    "SELECT orderkey, status, ds FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES "
                            + "(CAST(5 AS BIGINT), CAST('partial' AS VARCHAR), CAST('2026-07-04' AS VARCHAR)), "
                            + "(CAST(6 AS BIGINT), CAST('keep' AS VARCHAR), CAST('2026-07-04' AS VARCHAR))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }

    @Test
    public void testMinioHashDynamicWriteSmoke()
    {
        String unpartitionedTable = "hash_dynamic_" + randomNameSuffix();
        String partitionedTable = "hash_dynamic_partitioned_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedUnpartitionedTable = qualifiedSchemaName + "." + unpartitionedTable;
        String qualifiedPartitionedTable = qualifiedSchemaName + "." + partitionedTable;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertUpdate("CREATE TABLE " + qualifiedUnpartitionedTable + " ("
                    + "id integer, "
                    + "name varchar) "
                    + "WITH ("
                    + "primary_key = ARRAY['id'], "
                    + "bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2')");
            assertUpdate("INSERT INTO " + qualifiedUnpartitionedTable + " VALUES "
                    + "(1, 'old'), "
                    + "(2, 'stale')", 2);
            assertQuery(
                    "SELECT id, name FROM " + qualifiedUnpartitionedTable + " ORDER BY id",
                    "VALUES (1, 'old'), (2, 'stale')");

            Session overwriteSession = Session.builder(getSession())
                    .setCatalogSessionProperty(CATALOG, PaimonSessionProperties.INSERT_EXISTING_PARTITIONS_BEHAVIOR, "overwrite")
                    .build();
            assertUpdate(
                    overwriteSession,
                    "INSERT INTO " + qualifiedUnpartitionedTable + " VALUES "
                            + "(3, 'new'), "
                            + "(4, 'fresh')",
                    2);
            assertQuery(
                    "SELECT id, name FROM " + qualifiedUnpartitionedTable + " ORDER BY id",
                    "VALUES (3, 'new'), (4, 'fresh')");

            assertUpdate("CREATE TABLE " + qualifiedPartitionedTable + " ("
                    + "ds varchar, "
                    + "id integer, "
                    + "name varchar) "
                    + "WITH ("
                    + "partitioned_by = ARRAY['ds'], "
                    + "primary_key = ARRAY['ds', 'id'], "
                    + "bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2')");
            assertUpdate("INSERT INTO " + qualifiedPartitionedTable + " VALUES "
                    + "('2026-07-01', 1, 'one'), "
                    + "('2026-07-01', 2, 'two'), "
                    + "('2026-07-02', 3, 'three'), "
                    + "('2026-07-02', 4, 'four')", 4);
            assertQuery(
                    "SELECT ds, id, name FROM " + qualifiedPartitionedTable + " ORDER BY ds, id",
                    "VALUES "
                            + "('2026-07-01', 1, 'one'), "
                            + "('2026-07-01', 2, 'two'), "
                            + "('2026-07-02', 3, 'three'), "
                            + "('2026-07-02', 4, 'four')");

            assertThat(listObjects(WAREHOUSE_PREFIX + "/" + SCHEMA + ".db/" + unpartitionedTable))
                    .anyMatch(path -> !path.endsWith("_trino_paimon_directory_marker"));
            assertThat(listObjects(WAREHOUSE_PREFIX + "/" + SCHEMA + ".db/" + partitionedTable))
                    .anyMatch(path -> !path.endsWith("_trino_paimon_directory_marker"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedPartitionedTable);
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedUnpartitionedTable);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }

    @Test
    public void testMinioKeyDynamicAtomicWriteSmoke()
    {
        String tableName = "key_dynamic_atomic_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "dt integer, "
                    + "id integer, "
                    + "value varchar) WITH ("
                    + "partitioned_by = ARRAY['dt'], "
                    + "primary_key = ARRAY['id'], "
                    + "bucket = '-1', "
                    + "dynamic_bucket_assigner_parallelism = '2', "
                    + "dynamic_bucket_initial_buckets = '2', "
                    + "cross_partition_upsert_bootstrap_parallelism = '2')");

            assertUpdate("INSERT INTO " + qualifiedTableName
                    + " SELECT CAST(n % 16 AS INTEGER), CAST(n AS INTEGER), "
                    + "CAST('initial-' || CAST(n AS VARCHAR) AS VARCHAR) "
                    + "FROM UNNEST(sequence(1, 10000)) AS t(n)", 10000);

            assertUpdate("INSERT INTO " + qualifiedTableName
                    + " SELECT CAST((n + 7) % 32 AS INTEGER), CAST(n AS INTEGER), "
                    + "CAST('updated-' || CAST(n AS VARCHAR) AS VARCHAR) "
                    + "FROM UNNEST(sequence(1, 1000)) AS t(n)"
                    + " UNION ALL SELECT CAST((n + 7) % 32 AS INTEGER), CAST(n AS INTEGER), "
                    + "CAST('new-' || CAST(n AS VARCHAR) AS VARCHAR) "
                    + "FROM UNNEST(sequence(10001, 11000)) AS t(n)", 2000);

            assertQuery("SELECT count(*) FROM " + qualifiedTableName, "VALUES CAST(11000 AS BIGINT)");
            assertQuery("SELECT count(DISTINCT id) FROM " + qualifiedTableName, "VALUES CAST(11000 AS BIGINT)");
            assertQuery(
                    "SELECT dt, id, value FROM " + qualifiedTableName + " WHERE id IN (1, 10001) ORDER BY id",
                    "VALUES (CAST(8 AS INTEGER), 1, CAST('updated-1' AS VARCHAR)), "
                            + "(CAST(24 AS INTEGER), 10001, CAST('new-10001' AS VARCHAR))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }

    @Test
    public void testMinioSpillableWriteBufferSmoke()
    {
        String tableName = "spillable_write_" + randomNameSuffix();
        String qualifiedSchemaName = CATALOG + "." + SCHEMA;
        String qualifiedTableName = qualifiedSchemaName + "." + tableName;

        assertUpdate("CREATE SCHEMA " + qualifiedSchemaName);
        try {
            assertUpdate("CREATE TABLE " + qualifiedTableName + " ("
                    + "orderkey bigint, "
                    + "status varchar, "
                    + "ds varchar) "
                    + "WITH ("
                    + "partitioned_by = ARRAY['ds'], "
                    + "write_buffer_for_append = 'true', "
                    + "write_buffer_spillable = 'true', "
                    + "write_max_writers_to_spill = '1')");

            assertUpdate("INSERT INTO " + qualifiedTableName + " VALUES "
                    + "(1, 'queued', '2026-07-01'), "
                    + "(2, 'ready', '2026-07-02')", 2);

            assertQuery(
                    "SELECT orderkey, status, ds FROM " + qualifiedTableName + " ORDER BY orderkey",
                    "VALUES "
                            + "(CAST(1 AS BIGINT), CAST('queued' AS VARCHAR), CAST('2026-07-01' AS VARCHAR)), "
                            + "(CAST(2 AS BIGINT), CAST('ready' AS VARCHAR), CAST('2026-07-02' AS VARCHAR))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + qualifiedTableName);
            assertUpdate("DROP SCHEMA IF EXISTS " + qualifiedSchemaName);
        }
    }

    private List<String> listObjects(String prefix)
    {
        try (MinioClient client = MinioClient.builder()
                .endpoint(minio.getMinioAddress())
                .credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
                .build()) {
            List<String> objectNames = new ArrayList<>();
            for (Result<Item> result : client.listObjects(ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(prefix)
                    .recursive(true)
                    .build())) {
                try {
                    objectNames.add(result.get().objectName());
                }
                catch (Exception e) {
                    throw new RuntimeException("Failed to list MinIO objects", e);
                }
            }
            return objectNames;
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to list MinIO objects", e);
        }
    }
}
