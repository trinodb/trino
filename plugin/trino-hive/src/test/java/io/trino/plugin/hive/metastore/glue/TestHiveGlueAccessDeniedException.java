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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.plugin.hive.FlociS3AndGlue;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore.TableKind;
import io.trino.spi.catalog.CatalogName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;

import java.nio.file.Path;
import java.util.EnumSet;

import static io.trino.plugin.hive.metastore.glue.GlueMetastoreModule.createGlueClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHiveGlueAccessDeniedException
        extends AbstractTestQueryFramework
{
    private static final String ACCESS_DENIED_DATABASE = "access_denied_database";
    private static final String ACCESS_DENIED_TABLE = "access_denied_table";

    private final String schemaName = "test_hive_glue_" + randomNameSuffix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        FlociS3AndGlue floci = closeAfterClass(new FlociS3AndGlue());
        String bucketName = "test-hive-glue-access-denied-" + randomNameSuffix();
        floci.createBucket(bucketName);

        ExecutionInterceptor denyGetTable = new ExecutionInterceptor()
        {
            @Override
            public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes)
            {
                if (context.request() instanceof GetDatabaseRequest request && request.name().equals(ACCESS_DENIED_DATABASE)) {
                    throw AccessDeniedException.builder().build();
                }

                if (context.request() instanceof GetTableRequest request && request.name().equals(ACCESS_DENIED_TABLE)) {
                    throw AccessDeniedException.builder().build();
                }
            }
        };

        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setGlueRegion(FLOCI_REGION)
                .setDefaultWarehouseDir("s3://%s/".formatted(bucketName));
        floci.configureGlueHiveMetastore(glueConfig);
        GlueClient glueClient = createGlueClient(glueConfig, ImmutableSet.of(denyGetTable));

        GlueHiveMetastore metastore = new GlueHiveMetastore(
                glueClient,
                GlueCache.NOOP,
                new GlueMetastoreStats(),
                floci.createFileSystemFactory(),
                glueConfig,
                new CatalogName("hive"),
                EnumSet.allOf(TableKind.class));

        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema(schemaName)
                .build();
        QueryRunner queryRunner = new StandaloneQueryRunner(session);

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");
        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory, metastore));

        queryRunner.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("fs.s3.enabled", "true")
                .putAll(floci.s3Properties())
                .buildOrThrow());

        queryRunner.execute("CREATE SCHEMA " + schemaName + " WITH (location = 's3://%s/%s')".formatted(bucketName, schemaName));
        return queryRunner;
    }

    @Test
    void testGetDatabaseAccessDeniedException()
    {
        String schemaName = "hive.%s".formatted(ACCESS_DENIED_DATABASE);

        assertThat(query("SHOW CREATE SCHEMA " + schemaName)).failure()
                .hasMessage("line 1:1: Schema '%s' does not exist", schemaName);
    }

    @Test
    void testGetTableAccessDeniedException()
    {
        String tableName = "hive.%s.%s".formatted(schemaName, ACCESS_DENIED_TABLE);

        assertThat(query("SELECT * FROM " + tableName)).failure()
                .hasMessage("line 1:15: Table '%s' does not exist", tableName);
    }
}
