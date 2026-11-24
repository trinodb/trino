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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.biglake.hive.v1.CatalogName;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceSettings;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonMapperProvider;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.metastore.lakehouse.TpchLakehouseTablesInitializer;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.util.Base64;
import java.util.Map;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveLakehouseMetastoreConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private static final String SCHEMA = "test_hive_lakehouse_" + randomNameSuffix();
    private static final String GCP_STORAGE_BUCKET = requireEnv("GCP_STORAGE_BUCKET");
    private static final String GCP_LAKEHOUSE_CATALOG_ID = requireEnv("GCP_LAKEHOUSE_CATALOG_ID");
    private static final byte[] GCS_JSON_KEY_BYTES = Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY"));
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private final TpchLakehouseTablesInitializer tablesInitializer = new TpchLakehouseTablesInitializer(REQUIRED_TPCH_TABLES);
    private HiveMetastoreServiceClient client;
    private CatalogName catalogName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JsonNode gcsJson = JSON_MAPPER.readTree(GCS_JSON_KEY_BYTES);
        String projectId = gcsJson.get("project_id").asString();
        catalogName = CatalogName.of(projectId, GCP_LAKEHOUSE_CATALOG_ID);
        client = closeAfterClass(HiveMetastoreServiceClient.create(HiveMetastoreServiceSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new ByteArrayInputStream(GCS_JSON_KEY_BYTES))
                        .createScoped(HiveMetastoreServiceSettings.getDefaultServiceScopes())))
                .build()));

        Map<String, String> fileSystemProperties = ImmutableMap.of(
                "fs.gcs.enabled", "true",
                "gcs.json-key", gcsJson.toString());

        QueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                        .setCatalog("hive")
                        .setSchema(SCHEMA)
                        .build())
                .addHiveProperty("hive.metastore", "lakehouse")
                .addHiveProperty("hive.metastore.lakehouse.project-id", projectId)
                .addHiveProperty("hive.metastore.lakehouse.catalog-id", GCP_LAKEHOUSE_CATALOG_ID)
                .addHiveProperty("hive.metastore.lakehouse.google-json-key", gcsJson.toString())
                .addHiveProperty("hive.security", "allow-all")
                .addHiveProperties(fileSystemProperties)
                .setCreateTpchSchemas(false) // The Lakehouse runtime catalog is read-only, so the tables initializer creates the tables before the tests run
                .build();

        tablesInitializer.initializeTables(queryRunner, fileSystemProperties, client, catalogName, Location.of("gs://%s/%s".formatted(GCP_STORAGE_BUCKET, SCHEMA)), SCHEMA);
        return queryRunner;
    }

    @AfterAll
    void cleanup()
    {
        tablesInitializer.dropTables(getQueryRunner(), client, catalogName, SCHEMA);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_INSERT,
                 SUPPORTS_DELETE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_COMMENT_ON_COLUMN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("This Lakehouse runtime catalog supports read operations only, not creating tables");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("This Lakehouse runtime catalog supports read operations only, not creating tables");
    }

    @Test
    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessageContaining("Cannot write to non-managed Hive table");
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("This Lakehouse runtime catalog supports read operations only, not creating databases");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("This Lakehouse runtime catalog supports read operations only, not renaming databases");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessageContaining("Table rename is not supported with current metastore configuration");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("This Lakehouse runtime catalog supports read operations only, not creating tables");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Override because the tables are external tables
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("\\QCREATE TABLE hive." + SCHEMA + ".region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   external_location = \\E'.*/region',\n\\Q" +
                        "   format = 'PARQUET'\n" +
                        ")");
    }
}
