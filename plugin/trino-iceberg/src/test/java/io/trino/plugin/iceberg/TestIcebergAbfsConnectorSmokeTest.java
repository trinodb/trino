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
import io.trino.filesystem.Location;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.FileFormat.ORC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergAbfsConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String container;
    private final String account;
    private final String accessKey;
    private final String schemaName;
    private final String bucketName;

    private HiveHadoop hiveHadoop;

    public TestIcebergAbfsConnectorSmokeTest()
    {
        super(ORC);
        this.container = requiredNonEmptySystemProperty("testing.azure-abfs-container");
        this.account = requiredNonEmptySystemProperty("testing.azure-abfs-account");
        this.accessKey = requiredNonEmptySystemProperty("testing.azure-abfs-access-key");
        this.schemaName = "tpch_" + format.name().toLowerCase(ENGLISH);
        this.bucketName = "test-iceberg-smoke-test-" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String abfsSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("hdp3.1-core-site.xml.abfs-template"), UTF_8)
                .replace("%ABFS_ACCESS_KEY%", accessKey)
                .replace("%ABFS_ACCOUNT%", account);

        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path hadoopCoreSiteXmlTempFile = java.nio.file.Files.createTempFile("core-site", ".xml", posixFilePermissions);
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        java.nio.file.Files.writeString(hadoopCoreSiteXmlTempFile, abfsSpecificCoreSiteXmlContent);

        this.hiveHadoop = closeAfterClass(HiveHadoop.builder()
                .withImage(HiveHadoop.HIVE3_IMAGE)
                .withFilesToMount(ImmutableMap.of("/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString()))
                .build());
        this.hiveHadoop.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "HIVE_METASTORE")
                                .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                                .put("hive.metastore.thrift.client.read-timeout", "1m") // read timed out sometimes happens with the default timeout
                                .put("fs.hadoop.enabled", "false")
                                .put("fs.native-azure.enabled", "true")
                                .put("azure.auth-type", "ACCESS_KEY")
                                .put("azure.access-key", accessKey)
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                                .buildOrThrow())
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(schemaName)
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaProperties(Map.of("location", "'" + formatAbfsUrl(container, account, bucketName) + schemaName + "'"))
                                .build())
                .build();
    }

    @Override
    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName + " WITH (location = '" + formatAbfsUrl(container, account, bucketName) + schemaName + "')";
    }

    @Test
    @Override
    public void testRenameSchema()
    {
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
        metastore.dropTable(schemaName, tableName, false);
        assertThat(metastore.getTable(schemaName, tableName)).isEmpty();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                        .build(this::closeAfterClass));
        return metastore
                .getTable(schemaName, tableName).orElseThrow()
                .getParameters().get("metadata_location");
    }

    @Override
    protected String schemaPath()
    {
        return formatAbfsUrl(container, account, bucketName) + schemaName;
    }

    @Override
    protected boolean locationExists(String location)
    {
        return hiveHadoop.executeInContainer("hadoop", "fs", "-test", "-d", location).getExitCode() == 0;
    }

    @Override
    protected void deleteDirectory(String location)
    {
        hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-rm", "-f", "-r", location);
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    private static String formatAbfsUrl(String container, String account, String bucketName)
    {
        return format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, bucketName);
    }
}
