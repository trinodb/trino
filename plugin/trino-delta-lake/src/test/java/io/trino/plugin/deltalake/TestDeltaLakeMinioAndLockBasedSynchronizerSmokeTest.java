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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.deltalake.transactionlog.writer.S3LockBasedTransactionLogSynchronizer;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Delta Lake connector smoke test exercising Hive metastore and MinIO storage with exclusive create support disabled.
 */
public class TestDeltaLakeMinioAndLockBasedSynchronizerSmokeTest
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_schema";

    protected final String bucketName = "test-bucket-" + randomNameSuffix();
    protected MinioClient minioClient;
    protected Minio minio;
    protected HiveMetastore metastore;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);
        minioClient = closeAfterClass(minio.createMinioClient());

        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(DELTA_CATALOG)
                        .setSchema(SCHEMA)
                        .build())
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(new DeltaLakePlugin());
            queryRunner.createCatalog(DELTA_CATALOG, DeltaLakeConnectorFactory.CONNECTOR_NAME, ImmutableMap.<String, String>builder()
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("file-metastore").toString())
                    .put("hive.metastore.disable-location-checks", "true")
                    // required by the file metastore
                    .put("fs.hadoop.enabled", "true")
                    .put("fs.native-s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                    .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                    .put("s3.region", MINIO_REGION)
                    .put("s3.endpoint", minio.getMinioAddress())
                    .put("s3.path-style-access", "true")
                    .put("s3.streaming.part-size", "5MB") // minimize memory usage
                    .put("s3.exclusive-create", "false") // disable so we can test our own locking scheme
                    .put("delta.metastore.store-table-metadata", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .put("delta.register-table-procedure.enabled", "true")
                    .buildOrThrow());
            metastore = TestingDeltaLakeUtils.getConnectorService(queryRunner, HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            queryRunner.execute("CREATE SCHEMA " + SCHEMA + " WITH (location = 's3://" + bucketName + "/" + SCHEMA + "')");
            metastore = TestingDeltaLakeUtils.getConnectorService(queryRunner, HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @Test
    public void testWritesLocked()
            throws Exception
    {
        testWritesLocked("INSERT INTO %s VALUES (3, 'kota'), (4, 'psa')");
        testWritesLocked("UPDATE %s SET a_string = 'kota' WHERE a_number = 2");
        testWritesLocked("DELETE FROM %s WHERE a_number = 1");
    }

    private void testWritesLocked(String writeStatement)
            throws Exception
    {
        String tableName = "test_writes_locked" + randomNameSuffix();
        try {
            assertUpdate(
                    format("CREATE TABLE %s (a_number, a_string) WITH (location = 's3://%s/%s') AS " +
                                    "VALUES (1, 'ala'), (2, 'ma')",
                            tableName,
                            bucketName,
                            tableName),
                    2);

            Set<String> originalFiles = ImmutableSet.copyOf(getTableFiles(tableName));
            assertThat(originalFiles).isNotEmpty(); // sanity check

            String lockFilePath = lockTable(tableName, java.time.Duration.ofMinutes(5));
            assertThatThrownBy(() -> computeActual(format(writeStatement, tableName)))
                    .hasStackTraceContaining("Transaction log locked(1); lockingCluster=some_cluster; lockingQuery=some_query");
            assertThat(listLocks(tableName)).containsExactly(lockFilePath); // we should not delete exising, not-expired lock

            // files from failed write should be cleaned up
            Set<String> expectedFiles = ImmutableSet.<String>builder()
                    .addAll(originalFiles)
                    .add(lockFilePath)
                    .build();
            assertEventually(
                    new Duration(5, TimeUnit.SECONDS),
                    () -> assertThat(getTableFiles(tableName)).containsExactlyInAnyOrderElementsOf(expectedFiles));
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testWritesLockExpired()
            throws Exception
    {
        testWritesLockExpired("INSERT INTO %s VALUES (3, 'kota')", "VALUES (1,'ala'), (2,'ma'), (3,'kota')");
        testWritesLockExpired("UPDATE %s SET a_string = 'kota' WHERE a_number = 2", "VALUES (1,'ala'), (2,'kota')");
        testWritesLockExpired("DELETE FROM %s WHERE a_number = 2", "VALUES (1,'ala')");
    }

    private void testWritesLockExpired(String writeStatement, String expectedValues)
            throws Exception
    {
        String tableName = "test_writes_locked" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string) WITH (location = 's3://%s/%s') AS " +
                                "VALUES (1, 'ala'), (2, 'ma')",
                        tableName,
                        bucketName,
                        tableName),
                2);

        lockTable(tableName, java.time.Duration.ofSeconds(-5));
        assertUpdate(format(writeStatement, tableName), 1);
        assertQuery("SELECT * FROM " + tableName, expectedValues);
        assertThat(listLocks(tableName)).isEmpty(); // expired lock should be cleaned up

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testWritesLockInvalidContents()
    {
        testWritesLockInvalidContents("INSERT INTO %s VALUES (3, 'kota')", "VALUES (1,'ala'), (2,'ma'), (3,'kota')");
        testWritesLockInvalidContents("UPDATE %s SET a_string = 'kota' WHERE a_number = 2", "VALUES (1,'ala'), (2,'kota')");
        testWritesLockInvalidContents("DELETE FROM %s WHERE a_number = 2", "VALUES (1,'ala')");
    }

    private void testWritesLockInvalidContents(String writeStatement, String expectedValues)
    {
        String tableName = "test_writes_locked" + randomNameSuffix();
        assertUpdate(
                format("CREATE TABLE %s (a_number, a_string) WITH (location = 's3://%s/%s') AS " +
                                "VALUES (1, 'ala'), (2, 'ma')",
                        tableName,
                        bucketName,
                        tableName),
                2);

        String lockFilePath = invalidLockTable(tableName);
        assertUpdate(format(writeStatement, tableName), 1);
        assertQuery("SELECT * FROM " + tableName, expectedValues);
        assertThat(listLocks(tableName)).containsExactly(lockFilePath); // we should not delete unparsable lock file

        assertUpdate("DROP TABLE " + tableName);
    }

    private String lockTable(String tableName, java.time.Duration lockDuration)
            throws Exception
    {
        String lockFilePath = format("%s/00000000000000000001.json.sb-lock_blah", getLockFileDirectory(tableName));
        String lockFileContents = OBJECT_MAPPER.writeValueAsString(
                new S3LockBasedTransactionLogSynchronizer.LockFileContents("some_cluster", "some_query", Instant.now().plus(lockDuration).toEpochMilli()));
        minioClient.putObject(bucketName, lockFileContents.getBytes(UTF_8), lockFilePath);
        String lockUri = format("s3://%s/%s", bucketName, lockFilePath);
        assertThat(listLocks(tableName)).containsExactly(lockUri); // sanity check
        return lockUri;
    }

    private String invalidLockTable(String tableName)
    {
        String lockFilePath = format("%s/00000000000000000001.json.sb-lock_blah", getLockFileDirectory(tableName));
        String invalidLockFileContents = "some very wrong json contents";
        minioClient.putObject(bucketName, invalidLockFileContents.getBytes(UTF_8), lockFilePath);
        String lockUri = format("s3://%s/%s", bucketName, lockFilePath);
        assertThat(listLocks(tableName)).containsExactly(lockUri); // sanity check
        return lockUri;
    }

    private List<String> listLocks(String tableName)
    {
        List<String> paths = minioClient.listObjects(bucketName, getLockFileDirectory(tableName));
        return paths.stream()
                .filter(path -> path.contains(".sb-lock_"))
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    private String getLockFileDirectory(String tableName)
    {
        return format("%s/_delta_log/_sb_lock", tableName);
    }

    protected List<String> getTableFiles(String tableName)
    {
        return minioClient.listObjects(bucketName, tableName).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }
}
