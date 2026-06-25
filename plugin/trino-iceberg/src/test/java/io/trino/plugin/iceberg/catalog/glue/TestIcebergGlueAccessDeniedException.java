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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.multibindings.ProvidesIntoSet;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.Session;
import io.trino.metastore.Database;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.metastore.glue.ForGlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.iceberg.TestingIcebergPlugin;
import io.trino.plugin.iceberg.catalog.IcebergCatalogModule;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;

final class TestIcebergGlueAccessDeniedException
        extends AbstractTestQueryFramework
{
    private final String schemaName = "test_iceberg_glue_" + randomNameSuffix();

    private GlueHiveMetastore glueHiveMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema(schemaName)
                .build();
        QueryRunner queryRunner = new StandaloneQueryRunner(session);

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-iceberg-glue-access-denied-" + randomNameSuffix();
        floci.createBucket(bucketName);

        queryRunner.installPlugin(new TestingIcebergPlugin(dataDirectory, () -> Optional.of(new TestingGlueCatalogModule())));
        queryRunner.createCatalog("iceberg", "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "glue")
                .put("hive.metastore.glue.default-warehouse-dir", "s3://%s/".formatted(bucketName))
                .put("fs.s3.enabled", "true")
                .putAll(floci.s3AndGlueProperties())
                .buildOrThrow());

        glueHiveMetastore = getConnectorService(queryRunner, GlueHiveMetastore.class);

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
    void cleanup()
    {
        if (glueHiveMetastore != null) {
            glueHiveMetastore.dropDatabase(schemaName, false);
        }
    }

    @Test
    void testInformationSchemaColumns()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM information_schema.columns WHERE table_schema = '" + schemaName + "'");
    }

    @Test
    void testMetadataTableComments()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM system.metadata.table_comments WHERE schema_name = '" + schemaName + "'");
    }

    private static class TestingGlueCatalogModule
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
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes)
                {
                    if (context.request() instanceof GetTablesRequest) {
                        throw AccessDeniedException.builder().build();
                    }
                }
            };
        }
    }
}
