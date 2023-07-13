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
import io.trino.plugin.deltalake.transactionlog.writer.S3NativeTransactionLogSynchronizer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Delta Lake connector smoke test exercising Hive metastore and MinIO storage.
 */
public class TestDeltaLakeMinioAndHmsConnectorSmokeTest
        extends BaseDeltaLakeAwsConnectorSmokeTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    @Override
    protected Map<String, String> hiveStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("hive.s3.path-style-access", "true")
                .put("hive.s3.max-connections", "2")
                .buildOrThrow();
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress())
                .put("s3.path-style-access", "true")
                .put("s3.streaming.part-size", "5MB") // minimize memory usage
                .put("s3.max-connections", "4") // verify no leaks
                .put("delta.enable-non-concurrent-writes", "true")
                .buildOrThrow();
    }

    @Test(dataProvider = "writesLockedQueryProvider")
    public void testWritesLocked(String writeStatement)
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

    @DataProvider
    public static Object[][] writesLockedQueryProvider()
    {
        return new Object[][] {
                {"INSERT INTO %s VALUES (3, 'kota'), (4, 'psa')"},
                {"UPDATE %s SET a_string = 'kota' WHERE a_number = 2"},
                {"DELETE FROM %s WHERE a_number = 1"},
        };
    }

    @Test(dataProvider = "writesLockExpiredValuesProvider")
    public void testWritesLockExpired(String writeStatement, String expectedValues)
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

    @DataProvider
    public static Object[][] writesLockExpiredValuesProvider()
    {
        return new Object[][] {
                {"INSERT INTO %s VALUES (3, 'kota')", "VALUES (1,'ala'), (2,'ma'), (3,'kota')"},
                {"UPDATE %s SET a_string = 'kota' WHERE a_number = 2", "VALUES (1,'ala'), (2,'kota')"},
                {"DELETE FROM %s WHERE a_number = 2", "VALUES (1,'ala')"},
        };
    }

    @Test(dataProvider = "writesLockInvalidContentsValuesProvider")
    public void testWritesLockInvalidContents(String writeStatement, String expectedValues)
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

    @Test
    public void testDeltaColumnInvariant()
    {
        String tableName = "test_invariants_" + randomNameSuffix();
        hiveMinioDataLake.copyResources("databricks/invariants", tableName);
        assertUpdate("CALL system.register_table('%s', '%s', '%s')".formatted(SCHEMA, tableName, getLocationForTable(bucketName, tableName)));

        assertQuery("SELECT * FROM " + tableName, "VALUES 1");
        assertUpdate("INSERT INTO " + tableName + " VALUES(2)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1), (2)");

        assertThatThrownBy(() -> query("INSERT INTO " + tableName + " VALUES(3)"))
                .hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");
        assertThatThrownBy(() -> query("UPDATE " + tableName + " SET dummy = 3 WHERE dummy = 1"))
                .hasMessageContaining("Updating a table with a check constraint is not supported");

        assertQuery("SELECT * FROM " + tableName, "VALUES (1), (2)");
    }

    @Test
    public void testSchemaEvolutionOnTableWithColumnInvariant()
    {
        String tableName = "test_schema_evolution_on_table_with_column_invariant_" + randomNameSuffix();
        hiveMinioDataLake.copyResources("databricks/invariants", tableName);
        getQueryRunner().execute(format(
                "CALL system.register_table('%s', '%s', '%s')",
                SCHEMA,
                tableName,
                getLocationForTable(bucketName, tableName)));

        assertThatThrownBy(() -> query("INSERT INTO " + tableName + " VALUES(3)"))
                .hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN c INT");
        assertUpdate("COMMENT ON COLUMN " + tableName + ".c IS 'example column comment'");
        assertUpdate("COMMENT ON TABLE " + tableName + " IS 'example table comment'");

        assertThatThrownBy(() -> query("INSERT INTO " + tableName + " VALUES(3, 30)"))
                .hasMessageContaining("Check constraint violation: (\"dummy\" < 3)");

        assertUpdate("INSERT INTO " + tableName + " VALUES(2, 20)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, NULL), (2, 20)");
    }

    @DataProvider
    public static Object[][] writesLockInvalidContentsValuesProvider()
    {
        return new Object[][] {
                {"INSERT INTO %s VALUES (3, 'kota')", "VALUES (1,'ala'), (2,'ma'), (3,'kota')"},
                {"UPDATE %s SET a_string = 'kota' WHERE a_number = 2", "VALUES (1,'ala'), (2,'kota')"},
                {"DELETE FROM %s WHERE a_number = 2", "VALUES (1,'ala')"},
        };
    }

    private String lockTable(String tableName, java.time.Duration lockDuration)
            throws Exception
    {
        String lockFilePath = format("%s/00000000000000000001.json.sb-lock_blah", getLockFileDirectory(tableName));
        String lockFileContents = OBJECT_MAPPER.writeValueAsString(
                new S3NativeTransactionLogSynchronizer.LockFileContents("some_cluster", "some_query", Instant.now().plus(lockDuration).toEpochMilli()));
        hiveMinioDataLake.writeFile(lockFileContents.getBytes(UTF_8), lockFilePath);
        String lockUri = format("s3://%s/%s", bucketName, lockFilePath);
        assertThat(listLocks(tableName)).containsExactly(lockUri); // sanity check
        return lockUri;
    }

    private String invalidLockTable(String tableName)
    {
        String lockFilePath = format("%s/00000000000000000001.json.sb-lock_blah", getLockFileDirectory(tableName));
        String invalidLockFileContents = "some very wrong json contents";
        hiveMinioDataLake.writeFile(invalidLockFileContents.getBytes(UTF_8), lockFilePath);
        String lockUri = format("s3://%s/%s", bucketName, lockFilePath);
        assertThat(listLocks(tableName)).containsExactly(lockUri); // sanity check
        return lockUri;
    }

    private List<String> listLocks(String tableName)
    {
        List<String> paths = hiveMinioDataLake.listFiles(getLockFileDirectory(tableName));
        return paths.stream()
                .filter(path -> path.contains(".sb-lock_"))
                .map(path -> format("s3://%s/%s", bucketName, path))
                .collect(toImmutableList());
    }

    private String getLockFileDirectory(String tableName)
    {
        return format("%s/_delta_log/_sb_lock", tableName);
    }
}
