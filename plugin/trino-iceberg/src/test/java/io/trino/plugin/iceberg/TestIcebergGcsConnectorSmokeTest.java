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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergGcsConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestIcebergGcsConnectorSmokeTest.class);

    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
    private final String gcpStorageBucket;
    private final String gcpCredentialKey;
    private final String schema;

    private HiveHadoop hiveHadoop;

    public TestIcebergGcsConnectorSmokeTest()
    {
        super(ORC);
        this.gcpStorageBucket = requiredNonEmptySystemProperty("testing.gcp-storage-bucket");
        this.gcpCredentialKey = requiredNonEmptySystemProperty("testing.gcp-credentials-key");
        this.schema = "test_iceberg_gcs_connector_smoke_test_" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        Path gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json", READ_ONLY_PERMISSIONS);
        gcpCredentialsFile.toFile().deleteOnExit();
        Files.write(gcpCredentialsFile, jsonKeyBytes);
        String gcpCredentials = new String(jsonKeyBytes, UTF_8);

        String gcpSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("hdp3.1-core-site.xml.gcs-template"), UTF_8)
                .replace("%GCP_CREDENTIALS_FILE_PATH%", "/etc/hadoop/conf/gcp-credentials.json");

        Path hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", READ_ONLY_PERMISSIONS);
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.writeString(hadoopCoreSiteXmlTempFile, gcpSpecificCoreSiteXmlContent);

        this.hiveHadoop = closeAfterClass(HiveHadoop.builder()
                .withImage(HIVE3_IMAGE)
                .withFilesToMount(ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString(),
                        "/etc/hadoop/conf/gcp-credentials.json", gcpCredentialsFile.toAbsolutePath().toString()))
                .build());
        this.hiveHadoop.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "hive_metastore")
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-gcs.enabled", "true")
                        .put("gcs.json-key", gcpCredentials)
                        .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                        .put("iceberg.file-format", format.name())
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.writer-sort-buffer-size", "1MB")
                        .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                        .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schema)
                                .withSchemaProperties(ImmutableMap.of("location", "'" + schemaPath() + "'"))
                                .build())
                .build();
    }

    @AfterAll
    public void removeTestData()
    {
        try {
            fileSystem.deleteDirectory(Location.of(schemaPath()));
        }
        catch (IOException e) {
            // The GCS bucket should be configured to expire objects automatically. Clean up issues do not need to fail the test.
            LOG.warn(e, "Failed to clean up GCS test directory: %s", schemaPath());
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
                // GCS tests use the Hive Metastore catalog which does not support renaming schemas
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected String createSchemaSql(String schema)
    {
        return format("CREATE SCHEMA %1$s WITH (location = '%2$s%1$s')", schema, schemaPath());
    }

    @Override
    protected String schemaPath()
    {
        return format("gs://%s/%s/", gcpStorageBucket, schema);
    }

    @Override
    protected boolean locationExists(String location)
    {
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertQueryFails(
                format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                "Hive metastore does not support renaming schemas");
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        metastore.dropTable(schema, tableName, false);
        assertThat(metastore.getTable(schema, tableName)).isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        return metastore
                .getTable(schema, tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }
}
