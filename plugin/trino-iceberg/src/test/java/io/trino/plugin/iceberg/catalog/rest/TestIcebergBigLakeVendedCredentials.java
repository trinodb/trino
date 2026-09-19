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
package io.trino.plugin.iceberg.catalog.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonMapperProvider;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountAuth;
import io.trino.filesystem.gcs.GcsServiceAccountAuthConfig;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Base64;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

// Not a BaseIcebergConnectorSmokeTest: that full suite takes too long against real Google Lakehouse Catalog.
@Execution(SAME_THREAD) // to prevent exceeding BigLake requests quota
final class TestIcebergBigLakeVendedCredentials
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "test_iceberg_biglake_vended_credentials_" + randomNameSuffix();
    private static final String GCP_CREDENTIALS_VENDING_STORAGE_BUCKET = requireEnv("GCP_CREDENTIALS_VENDING_STORAGE_BUCKET");
    private static final byte[] GCS_JSON_KEY_BYTES = Base64.getDecoder().decode(requireEnv("GCP_CREDENTIALS_KEY"));
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private TrinoFileSystem fileSystem;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        JsonNode gcsJson = JSON_MAPPER.readTree(GCS_JSON_KEY_BYTES);
        String projectId = gcsJson.get("project_id").asText();

        return IcebergQueryRunner.builder(SCHEMA)
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", "gs://" + GCP_CREDENTIALS_VENDING_STORAGE_BUCKET)
                .addIcebergProperty("iceberg.rest-catalog.security", "GOOGLE")
                .addIcebergProperty("iceberg.rest-catalog.google-project-id", projectId)
                .addIcebergProperty("iceberg.rest-catalog.view-endpoints-enabled", "false")
                .addIcebergProperty("iceberg.rest-catalog.server-assigned-table-location-enabled", "true")
                .addIcebergProperty("iceberg.rest-catalog.vended-credentials-enabled", "true")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("fs.gcs.enabled", "true")
                // Table data access uses vended per-table credentials, so no static GCS file system credentials are configured
                .addIcebergProperty("gcs.auth-type", "APPLICATION_DEFAULT")
                // Used for REST catalog (GOOGLE security) authentication
                .addIcebergProperty("iceberg.rest-catalog.google-json-key", gcsJson.toString())
                .setSchemaInitializer(SchemaInitializer.builder()
                        .withSchemaName(SCHEMA)
                        .withSchemaProperties(ImmutableMap.of("location", "'gs://%s/%s'".formatted(GCP_CREDENTIALS_VENDING_STORAGE_BUCKET, SCHEMA)))
                        .build())
                .build();
    }

    @BeforeAll
    void initFileSystem()
            throws Exception
    {
        String jsonKey = JSON_MAPPER.readTree(GCS_JSON_KEY_BYTES).toString();
        GcsFileSystemConfig config = new GcsFileSystemConfig();
        GcsServiceAccountAuth auth = new GcsServiceAccountAuth(new GcsServiceAccountAuthConfig().setJsonKey(jsonKey));
        fileSystem = new GcsFileSystemFactory(config, new GcsStorageFactory(config, auth)).create(SESSION);
    }

    @AfterAll
    void cleanup()
            throws Exception
    {
        getQueryRunner().execute("DROP SCHEMA " + SCHEMA);
        fileSystem.deleteDirectory(Location.of("gs://%s/%s".formatted(GCP_CREDENTIALS_VENDING_STORAGE_BUCKET, SCHEMA)));
    }

    @Test
    void testCreateTableAsSelectAndSelect()
    {
        try (TestTable table = newTrinoTable("test_ctas", "AS SELECT * FROM tpch.tiny.nation")) {
            assertThat(query("SELECT count(*) FROM " + table.getName()))
                    .matches("SELECT count(*) FROM tpch.tiny.nation");
        }
    }

    @Test
    void testDataModificationStatements()
    {
        try (TestTable table = newTrinoTable("test_dml", "(id bigint, value varchar)")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'INDIA'), (2, 'POLAND')", 2);
            assertUpdate("UPDATE " + tableName + " SET value = 'FRANCE' WHERE id = 1", 1);
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            assertUpdate(
                    "MERGE INTO " + tableName + " t USING (VALUES (1, 'GERMANY'), (3, 'SPAIN')) AS s(id, value) " +
                            "ON t.id = s.id " +
                            "WHEN MATCHED THEN UPDATE SET value = s.value " +
                            "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
                    2);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'GERMANY'), (3, 'SPAIN')");
        }
    }
}
