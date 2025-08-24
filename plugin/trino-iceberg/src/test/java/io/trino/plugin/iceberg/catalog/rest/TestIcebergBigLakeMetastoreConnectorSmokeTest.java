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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergBigLakeMetastoreConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final String GCP_STORAGE_BUCKET = requireEnv("GCP_STORAGE_BUCKET");
    private static final String GCP_CREDENTIALS_KEY = requireEnv("GCP_CREDENTIALS_KEY");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    public TestIcebergBigLakeMetastoreConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        byte[] jsonKeyBytes = Base64.getDecoder().decode(GCP_CREDENTIALS_KEY);
        Path gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json");
        gcpCredentialsFile.toFile().deleteOnExit();
        Files.write(gcpCredentialsFile, jsonKeyBytes);
        String projectId = OBJECT_MAPPER.readTree(jsonKeyBytes).get("project_id").asText();

        return IcebergQueryRunner.builder("tpch")
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", "https://biglake.googleapis.com/iceberg/v1beta/restcatalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", "gs://" + GCP_STORAGE_BUCKET)
                .addIcebergProperty("iceberg.rest-catalog.security", "GOOGLE")
                .addIcebergProperty("iceberg.rest-catalog.google-project-id", projectId)
                .addIcebergProperty("iceberg.rest-catalog.view-endpoints-enabled", "false")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                .addIcebergProperty("fs.native-gcs.enabled", "true")
                .addIcebergProperty("gcs.json-key-file-path", gcpCredentialsFile.toString())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        BaseTable table = trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return "dummy";
    }

    @Override
    protected boolean locationExists(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void deleteDirectory(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Test
    @Override // Override because the location pattern differs
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("CREATE TABLE iceberg.tpch.region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = 2,\n" +
                        "   location = 'gs://.*',\n" +
                        "   max_commit_retry = 4\n" +
                        "\\)");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasStackTraceContaining("Unable to process: RenameTable endpoint is not supported for Glue Catalog");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }
}
