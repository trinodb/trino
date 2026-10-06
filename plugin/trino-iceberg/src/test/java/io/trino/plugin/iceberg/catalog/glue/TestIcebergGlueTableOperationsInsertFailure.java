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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Database;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.metastore.glue.ForGlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergGlueTableOperationsInsertFailure
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergGlueTableOperationsInsertFailure.class);

    private static final String ICEBERG_CATALOG = "iceberg";

    private final String schemaName = "test_iceberg_glue_" + randomNameSuffix();
    private final AtomicReference<RuntimeException> updateTableFailure = new AtomicReference<>();
    private final AtomicBoolean updateTableAppliedBeforeFailure = new AtomicBoolean();

    private String bucketName;
    private GlueHiveMetastore glueHiveMetastore;
    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema(schemaName)
                .build();
        QueryRunner queryRunner = new StandaloneQueryRunner(session);

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        bucketName = "test-iceberg-glue-insert-failure-" + randomNameSuffix();
        floci.createBucket(bucketName);

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory, () -> Optional.of(new TestingGlueCatalogModule())));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "glue")
                .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .put("fs.s3.enabled", "true")
                .putAll(floci.s3AndGlueProperties())
                .buildOrThrow());

        glueHiveMetastore = getConnectorService(queryRunner, GlueHiveMetastore.class);
        fileSystem = getFileSystemFactory(queryRunner).create(ConnectorIdentity.ofUser("test"));

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .setLocation(Optional.of("s3://%s/%s".formatted(bucketName, schemaName)))
                .build();
        glueHiveMetastore.createDatabase(database);

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueHiveMetastore != null) {
                glueHiveMetastore.dropDatabase(schemaName, false);
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", schemaName);
        }
    }

    @BeforeEach
    public void resetGlueBehavior()
    {
        updateTableFailure.set(null);
        updateTableAppliedBeforeFailure.set(false);
    }

    @Test
    public void testInsertSucceedsWhenAppliedUpdateReportsFailure()
    {
        String tableName = "test_insert_failure" + randomNameSuffix();

        getQueryRunner().execute(format("CREATE TABLE %s (a_varchar) AS VALUES ('Trino')", tableName));
        updateTableAppliedBeforeFailure.set(true);
        updateTableFailure.set(new RuntimeException("Test-simulated Glue timeout exception"));

        assertUpdate("INSERT INTO " + tableName + " VALUES 'rocks'", 1);

        assertQuery("SELECT * FROM " + tableName, "VALUES 'Trino', 'rocks'");
    }

    @Test
    public void testInsertSucceedsWhenRetriedUpdateIsRejected()
            throws Exception
    {
        String tableName = "test_update_lost_response_" + randomNameSuffix();
        String tableLocation = "s3://" + bucketName + "/" + schemaName + "/" + tableName;
        getQueryRunner().execute("CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')");
        try {
            updateTableAppliedBeforeFailure.set(true);
            updateTableFailure.set(ConcurrentModificationException.builder().message("simulated ConcurrentModification from a retried update").build());

            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1");
            assertThat(metadataFiles(tableLocation)).hasSize(2);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertRetriedWhenRejectedWithoutConcurrentCommit()
            throws Exception
    {
        String tableName = "test_update_rejected_" + randomNameSuffix();
        String tableLocation = "s3://" + bucketName + "/" + schemaName + "/" + tableName;
        getQueryRunner().execute("CREATE TABLE " + tableName + " (a integer) WITH (location = '" + tableLocation + "')");
        try {
            updateTableFailure.set(ConcurrentModificationException.builder().message("simulated ConcurrentModification").build());

            assertUpdate("INSERT INTO " + tableName + " VALUES 1", 1);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1");
            assertThat(metadataFiles(tableLocation)).hasSize(3);
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private List<String> metadataFiles(String tableLocation)
            throws IOException
    {
        ImmutableList.Builder<String> files = ImmutableList.builder();
        FileIterator iterator = fileSystem.listFiles(Location.of(tableLocation + "/metadata"));
        while (iterator.hasNext()) {
            String location = iterator.next().location().toString();
            if (location.endsWith(".metadata.json")) {
                files.add(location);
            }
        }
        return files.build();
    }

    private class TestingGlueCatalogModule
            extends AbstractConfigurationAwareModule
    {
        @Override
        protected void setup(Binder binder)
        {
            install(new IcebergCatalogModule());
        }

        @ProvidesIntoSet
        @ForGlueHiveMetastore
        public ExecutionInterceptor createExecutionInterceptor()
        {
            return new ExecutionInterceptor()
            {
                @Override
                public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes)
                {
                    if (context.request() instanceof UpdateTableRequest && !updateTableAppliedBeforeFailure.get()) {
                        injectUpdateTableFailure();
                    }
                }

                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes)
                {
                    if (context.request() instanceof UpdateTableRequest && updateTableAppliedBeforeFailure.getAndSet(false)) {
                        injectUpdateTableFailure();
                    }
                }
            };
        }

        private void injectUpdateTableFailure()
        {
            RuntimeException failure = updateTableFailure.getAndSet(null);
            if (failure != null) {
                throw failure;
            }
        }
    }
}
