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
package io.trino.plugin.hive;

import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/trinodb/trino/issues/29428
 * Verifies that INSERT does not lose data when updateStatistics fails during commit.
 * The fix moves executeUpdateStatisticsOperations outside the try-catch that triggers rollback,
 * so statistics failures no longer cause data loss.
 */
public class TestInsertOverwriteWithStatsFailure
        extends AbstractTestQueryFramework
{
    private final AtomicBoolean failUpdateStatistics = new AtomicBoolean(false);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setMetastore(queryRunner -> {
                    Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");
                    dataDir.toFile().mkdirs();
                    HiveMetastore delegate = TestingFileHiveMetastore.createTestingFileHiveMetastore(
                            new LocalFileSystemFactory(dataDir),
                            Location.of("local:///hive"));
                    return createFailingStatsMetastore(delegate);
                })
                // Disable staging to use DIRECT_TO_TARGET_EXISTING_DIRECTORY mode,
                // which triggers updateTableStatistics during commit
                .addHiveProperty("hive.temporary-staging-directory-enabled", "false")
                .build();
    }

    private HiveMetastore createFailingStatsMetastore(HiveMetastore delegate)
    {
        return (HiveMetastore) Proxy.newProxyInstance(
                HiveMetastore.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                (_, method, args) -> {
                    if (method.getName().equals("updateTableStatistics") && failUpdateStatistics.get()) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Simulated failure in updateTableStatistics");
                    }
                    try {
                        return method.invoke(delegate, args);
                    }
                    catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    public void testInsertPreservesDataOnStatsUpdateFailure()
    {
        String tableName = "test_stats_failure_" + randomNameSuffix();

        // Create an unpartitioned table with initial data
        assertUpdate("CREATE TABLE " + tableName + " (name VARCHAR, id BIGINT)");
        assertUpdate("INSERT INTO " + tableName + " VALUES ('initial', 1)", 1);

        // Verify initial data
        assertThat(query("SELECT count(*) FROM " + tableName)).matches("VALUES BIGINT '1'");

        // Enable stats failure and perform INSERT
        // Without the fix: updateTableStatistics failure triggers rollback → INSERT fails
        // With the fix: failure is caught and logged → INSERT succeeds, data preserved
        failUpdateStatistics.set(true);
        assertUpdate("INSERT INTO " + tableName + " VALUES ('second', 2)", 1);
        failUpdateStatistics.set(false);

        // Verify all data is preserved
        assertThat(query("SELECT count(*) FROM " + tableName)).matches("VALUES BIGINT '2'");
        assertThat(query("SELECT name FROM " + tableName + " WHERE id = 2")).matches("VALUES VARCHAR 'second'");

        assertUpdate("DROP TABLE " + tableName);
    }
}
