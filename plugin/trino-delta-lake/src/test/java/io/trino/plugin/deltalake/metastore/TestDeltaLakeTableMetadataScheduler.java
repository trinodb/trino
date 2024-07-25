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
package io.trino.plugin.deltalake.metastore;

import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.reflect.Reflection.newProxy;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.time.temporal.ChronoUnit.SECONDS;

final class TestDeltaLakeTableMetadataScheduler
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("delta")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta");
        HiveMetastore metastore = createTestingFileHiveMetastore(dataDirectory.toFile());

        HiveMetastore proxiedMetastore = newProxy(HiveMetastore.class, (_, method, args) -> {
            try {
                if (method.getName().equals("replaceTable")) {
                    throw new UnsupportedOperationException();
                }
                return method.invoke(metastore, args);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });

        queryRunner.installPlugin(new TestingDeltaLakePlugin(dataDirectory, Optional.of(new TestingDeltaLakeMetastoreModule(proxiedMetastore))));
        queryRunner.createCatalog("delta", "delta_lake", ImmutableMap.of("delta.metastore.store-table-metadata", "true"));

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        queryRunner.execute("CREATE SCHEMA delta.default");

        return queryRunner;
    }

    @Test
    @Disabled // TODO Enable after fixing the flaky assertion with JMX
    void testFailureStopScheduler()
    {
        String coordinatorId = (String) computeScalar("SELECT node_id FROM system.runtime.nodes WHERE coordinator = true");

        IntStream.range(0, 11).forEach(i -> assertUpdate("CREATE TABLE test_" + i + "(x int) WITH (column_mapping_mode = 'name')"));

        assertQuery(
                "SELECT shutdown FROM jmx.current.\"trino.plugin.deltalake.metastore:catalog=delta,name=delta,type=deltalaketablemetadatascheduler\" " +
                        "WHERE node = '" + coordinatorId + "'",
                "VALUES false");

        // The max failure count is 10, so the scheduler should be stopped after 11 operations
        IntStream.range(0, 11).forEach(i -> {
            assertUpdate("ALTER TABLE test_" + i + " RENAME COLUMN x to y");
            assertUpdate("COMMENT ON TABLE test_" + i + " IS 'test comment'");
        });
        sleepUninterruptibly(Duration.of(1, SECONDS));

        assertQuery(
                "SELECT shutdown FROM jmx.current.\"trino.plugin.deltalake.metastore:catalog=delta,name=delta,type=deltalaketablemetadatascheduler\" " +
                        "WHERE node = '" + coordinatorId + "'",
                "VALUES true");

        // Metadata should return the correct values regardless of the scheduler status
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA",
                "VALUES " +
                        "('test_0', 'y'), " +
                        "('test_1', 'y'), " +
                        "('test_2', 'y'), " +
                        "('test_3', 'y'), " +
                        "('test_4', 'y'), " +
                        "('test_5', 'y'), " +
                        "('test_6', 'y'), " +
                        "('test_7', 'y'), " +
                        "('test_8', 'y'), " +
                        "('test_9', 'y'), " +
                        "('test_10', 'y')");
        assertQuery(
                "SELECT table_name, comment FROM system.metadata.table_comments WHERE schema_name = CURRENT_SCHEMA",
                "VALUES " +
                        "('test_0', 'test comment'), " +
                        "('test_1', 'test comment'), " +
                        "('test_2', 'test comment'), " +
                        "('test_3', 'test comment'), " +
                        "('test_4', 'test comment'), " +
                        "('test_5', 'test comment'), " +
                        "('test_6', 'test comment'), " +
                        "('test_7', 'test comment'), " +
                        "('test_8', 'test comment'), " +
                        "('test_9', 'test comment'), " +
                        "('test_10', 'test comment')");
    }
}
