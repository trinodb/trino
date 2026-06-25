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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonMapperProvider;
import io.airlift.units.Duration;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.deltalake.transactionlog.writer.S3LockBasedTransactionLogSynchronizer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Delta Lake connector smoke test exercising Hive metastore and S3 storage with exclusive create support disabled.
 */
public class TestDeltaLakeFlociAndLockBasedSynchronizerSmokeTest
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_schema";

    protected final String bucketName = "test-bucket-" + randomNameSuffix();
    protected Floci floci;
    protected HiveMetastore metastore;

    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        floci = closeAfterClass(new Floci());
        floci.start();
        floci.createBucket(bucketName);

        QueryRunner queryRunner = DeltaLakeQueryRunner.builder(SCHEMA)
                .addDeltaProperty("hive.metastore.catalog.dir", "local:///file-metastore")
                .addDeltaProperty("hive.metastore.disable-location-checks", "true")
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addS3Properties(floci, bucketName)
                .addDeltaProperty("delta.s3.transaction-log-conditional-writes.enabled", "false") // disable so we can test our own locking scheme
                .addDeltaProperty("delta.metastore.store-table-metadata", "true")
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .addDeltaProperty("delta.register-table-procedure.enabled", "true")
                .build();
        metastore = getConnectorService(queryRunner, HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

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
        String lockFileContents = JSON_MAPPER.writeValueAsString(
                new S3LockBasedTransactionLogSynchronizer.LockFileContents("some_cluster", "some_query", Instant.now().plus(lockDuration).toEpochMilli()));
        floci.putObject(bucketName, lockFileContents.getBytes(UTF_8), lockFilePath);
        String lockUri = format("s3://%s/%s", bucketName, lockFilePath);
        assertThat(listLocks(tableName)).containsExactly(lockUri); // sanity check
        return lockUri;
    }

    private String invalidLockTable(String tableName)
    {
        String lockFilePath = format("%s/00000000000000000001.json.sb-lock_blah", getLockFileDirectory(tableName));
        String invalidLockFileContents = "some very wrong json contents";
        floci.putObject(bucketName, invalidLockFileContents.getBytes(UTF_8), lockFilePath);
        String lockUri = format("s3://%s/%s", bucketName, lockFilePath);
        assertThat(listLocks(tableName)).containsExactly(lockUri); // sanity check
        return lockUri;
    }

    private List<String> listLocks(String tableName)
    {
        List<String> paths = floci.listObjects(bucketName, getLockFileDirectory(tableName));
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
        return floci.listObjects(bucketName, tableName).stream()
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }
}
