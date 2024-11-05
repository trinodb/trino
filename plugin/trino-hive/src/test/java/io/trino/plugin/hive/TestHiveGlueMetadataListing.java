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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.Session;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastore;
import io.trino.plugin.hive.metastore.glue.GlueHiveMetastoreConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.nio.file.Path;

import static io.trino.plugin.hive.metastore.glue.GlueMetastoreModule.createGlueClient;
import static io.trino.plugin.hive.metastore.glue.TestingGlueHiveMetastore.createTestingGlueHiveMetastore;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveGlueMetadataListing
        extends AbstractTestQueryFramework
{
    public static final String FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME = "failing_table_with_null_storage_descriptor";
    private static final Logger LOG = Logger.get(TestHiveGlueMetadataListing.class);
    private static final String HIVE_CATALOG = "hive";
    private final String tpchSchema = "test_tpch_schema_" + randomNameSuffix();
    private GlueHiveMetastore glueMetastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session hiveSession = testSessionBuilder()
                .setCatalog(HIVE_CATALOG)
                .setSchema(tpchSchema)
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(hiveSession).build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");
        dataDirectory.toFile().deleteOnExit();

        this.glueMetastore = createTestingGlueHiveMetastore(dataDirectory, this::closeAfterClass);
        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory, glueMetastore));
        queryRunner.createCatalog(HIVE_CATALOG, "hive", ImmutableMap.of("fs.hadoop.enabled", "true"));

        queryRunner.execute("CREATE SCHEMA " + tpchSchema + " WITH (location = '" + dataDirectory.toUri() + "')");
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION, TpchTable.NATION));

        createBrokenTable(dataDirectory);

        return queryRunner;
    }

    @AfterAll
    public void cleanup()
    {
        try {
            if (glueMetastore != null) {
                // Data is on the local disk and will be deleted by the deleteOnExit hook
                glueMetastore.dropDatabase(tpchSchema, false);
                glueMetastore.shutdown();
            }
        }
        catch (Exception e) {
            LOG.error(e, "Failed to clean up Glue database: %s", tpchSchema);
        }
    }

    @Test
    public void testReadInformationSchema()
    {
        String expectedTables = format("VALUES '%s', '%s', '%s'", TpchTable.REGION.getTableName(), TpchTable.NATION.getTableName(), FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME);

        assertThat(query("SELECT table_name FROM hive.information_schema.tables"))
                .skippingTypesCheck()
                .containsAll(expectedTables);
        assertThat(query("SELECT table_name FROM hive.information_schema.tables WHERE table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .matches(expectedTables);
        assertThat(query("SELECT table_name FROM hive.information_schema.tables WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'"))
                .skippingTypesCheck()
                .matches("VALUES 'region'");
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.tables WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME, tpchSchema));

        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_schema = '" + tpchSchema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment'), ('nation', 'nationkey'), ('nation', 'name'), ('nation', 'regionkey'), ('nation', 'comment')");
        assertQuery("SELECT table_name, column_name from hive.information_schema.columns WHERE table_name = 'region' AND table_schema='" + tpchSchema + "'",
                "VALUES ('region', 'regionkey'), ('region', 'name'), ('region', 'comment')");
        assertQueryReturnsEmptyResult(format("SELECT table_name FROM hive.information_schema.columns WHERE table_name = '%s' AND table_schema='%s'", FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME, tpchSchema));

        assertQuery("SHOW TABLES FROM hive." + tpchSchema, expectedTables);
    }

    private void createBrokenTable(Path dataDirectory)
    {
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(dataDirectory.toString());
        try (GlueClient glueClient = createGlueClient(glueConfig, OpenTelemetry.noop())) {
            TableInput tableInput = TableInput.builder()
                    .name(FAILING_TABLE_WITH_NULL_STORAGE_DESCRIPTOR_NAME)
                    .tableType("HIVE")
                    .build();

            CreateTableRequest createTableRequest = CreateTableRequest.builder()
                    .databaseName(tpchSchema)
                    .tableInput(tableInput)
                    .build();
            glueClient.createTable(createTableRequest);
        }
    }
}
