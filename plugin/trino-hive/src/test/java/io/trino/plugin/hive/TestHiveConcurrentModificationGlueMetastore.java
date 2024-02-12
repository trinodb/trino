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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.plugin.hive.metastore.glue.GlueCache;
import io.trino.plugin.hive.metastore.glue.GlueContext;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.spi.TrinoException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.metastore.glue.GlueMetastoreModule.createGlueClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHiveConcurrentModificationGlueMetastore
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "test_hive_concurrent";
    private static final String SCHEMA = "test_hive_glue_concurrent_" + randomNameSuffix();
    private Path dataDirectory;
    private GlueHiveMetastore metastore;
    private final AtomicBoolean failNextGlueUpdateTableCall = new AtomicBoolean(false);
    private final AtomicInteger updateTableCallsCounter = new AtomicInteger();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session deltaLakeSession = testSessionBuilder()
                .setCatalog(CATALOG_NAME)
                .setSchema(SCHEMA)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(deltaLakeSession).build();

        dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("data_hive_concurrent");
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(dataDirectory.toUri().toString());

        GlueClient glueClient = createGlueClient(new GlueHiveMetastoreConfig(), OpenTelemetry.noop());
        GlueClient proxiedGlueClient = newProxy(GlueClient.class, (proxy, method, args) -> {
            try {
                if (method.getName().equals("updateTable")) {
                    updateTableCallsCounter.incrementAndGet();
                    if (failNextGlueUpdateTableCall.get()) {
                        // Simulate concurrent modifications on the table that is about to be dropped
                        failNextGlueUpdateTableCall.set(false);
                        throw new TrinoException(HIVE_METASTORE_ERROR, ConcurrentModificationException.builder()
                                .message("Test-simulated metastore concurrent modification exception")
                                .build());
                    }
                }
                return method.invoke(glueClient, args);
            }
            catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });

        metastore = new GlueHiveMetastore(
                proxiedGlueClient,
                new GlueContext(glueConfig),
                GlueCache.NOOP,
                HDFS_FILE_SYSTEM_FACTORY,
                glueConfig,
                EnumSet.allOf(GlueHiveMetastore.TableKind.class));

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data"), metastore));
        queryRunner.createCatalog(CATALOG_NAME, "hive");
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);
        return queryRunner;
    }

    @Test
    public void testUpdateTableStatsWithConcurrentModifications()
    {
        String tableName = "test_glue_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 AS data", 1);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "   ('data',  null, 1.0, 0.0, null, 1, 1), " +
                        "   (null, null, null, null, 1.0, null, null)");

        failNextGlueUpdateTableCall.set(true);
        resetCounters();
        assertUpdate("INSERT INTO " + tableName + " VALUES 2", 1);
        assertThat(updateTableCallsCounter.get()).isEqualTo(2);
        assertQuery("SELECT * FROM " + tableName, "VALUES 1, 2");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "   ('data',  null, 1.0, 0.0, null, 1, 2), " +
                        "   (null, null, null, null, 2.0, null, null)");
    }

    private void resetCounters()
    {
        updateTableCallsCounter.set(0);
    }

    @AfterAll
    public void cleanup()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }
}
