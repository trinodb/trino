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
package io.trino.plugin.deltalake;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.google.common.reflect.ClassPath;
import io.trino.plugin.deltalake.util.DockerizedDataLake;
import io.trino.plugin.deltalake.util.TestingHadoop;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createAbfsDeltaLakeQueryRunner;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeAdlsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    private final String container;
    private final String account;
    private final String accessKey;
    private final BlobContainerClient azureContainerClient;
    private final String adlsDirectory;

    @Parameters({
            "hive.hadoop2.azure-abfs-container",
            "hive.hadoop2.azure-abfs-account",
            "hive.hadoop2.azure-abfs-access-key"})
    public TestDeltaLakeAdlsConnectorSmokeTest(String container, String account, String accessKey)
    {
        this.container = requireNonNull(container, "container is null");
        this.account = requireNonNull(account, "account is null");
        this.accessKey = requireNonNull(accessKey, "accessKey is null");

        String connectionString = format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net", account, accessKey);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        this.azureContainerClient = blobServiceClient.getBlobContainerClient(container);
        this.adlsDirectory = format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, bucketName);
    }

    @Test
    public void testDropSchemaExternalFiles()
    {
        // TODO move this test to base class, so it's exercised for S3 too

        String schemaName = "externalFileSchema";
        String schemaDir = fullAdlsUrl() + "drop-schema-with-external-files/";
        String subDir = schemaDir + "subdir/";
        String externalFile = subDir + "external-file";

        TestingHadoop hadoopContainer = dockerizedDataLake.getTestingHadoop();

        // Create file in a subdirectory of the schema directory before creating schema
        hadoopContainer.runCommandInContainer("hdfs", "dfs", "-mkdir", "-p", subDir);
        hadoopContainer.runCommandInContainer("hdfs", "dfs", "-touchz", externalFile);

        query(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertThat(hadoopContainer.executeInContainer("hdfs", "dfs", "-test", "-e", externalFile).getExitCode())
                .as("external file exists after creating schema")
                .isEqualTo(0);

        query("DROP SCHEMA " + schemaName);
        assertThat(hadoopContainer.executeInContainer("hdfs", "dfs", "-test", "-e", externalFile).getExitCode())
                .as("external file exists after dropping schema")
                .isEqualTo(0);

        // Test behavior without external file
        hadoopContainer.runCommandInContainer("hdfs", "dfs", "-rm", "-r", subDir);

        query(format("CREATE SCHEMA %s WITH (location = '%s')", schemaName, schemaDir));
        assertThat(hadoopContainer.executeInContainer("hdfs", "dfs", "-test", "-d", schemaDir).getExitCode())
                .as("schema directory exists after creating schema")
                .isEqualTo(0);

        query("DROP SCHEMA " + schemaName);
        assertThat(hadoopContainer.executeInContainer("hdfs", "dfs", "-test", "-e", externalFile).getExitCode())
                .as("schema directory deleted after dropping schema without external file")
                .isEqualTo(1);
    }

    @Override
    DockerizedDataLake createDockerizedDataLake()
            throws Exception
    {
        String abfsSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("io/trino/plugin/deltalake/hdp3.1-core-site.xml.abfs-template"), UTF_8)
                .replace("%ABFS_ACCESS_KEY%", accessKey)
                .replace("%ABFS_ACCOUNT%", account);

        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", posixFilePermissions);
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.write(hadoopCoreSiteXmlTempFile, abfsSpecificCoreSiteXmlContent.getBytes(UTF_8));

        return new DockerizedDataLake(
                getHadoopBaseImage(),
                ImmutableMap.of(),
                ImmutableMap.of(hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString(), "/etc/hadoop/conf/core-site.xml"));
    }

    @Override
    QueryRunner createDeltaLakeQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        return createAbfsDeltaLakeQueryRunner(DELTA_CATALOG, SCHEMA, ImmutableMap.of(), connectorProperties, dockerizedDataLake.getTestingHadoop());
    }

    @Override
    protected Optional<String> getHadoopBaseImage()
    {
        return Optional.of("ghcr.io/trinodb/testing/hdp3.1-hive");
    }

    @AfterClass(alwaysRun = true)
    public void removeTestData()
    {
        if (adlsDirectory != null && dockerizedDataLake.getTestingHadoop() != null) {
            dockerizedDataLake.getTestingHadoop().runCommandInContainer("hadoop", "fs", "-rm", "-f", "-r", adlsDirectory);
        }
        assertThat(azureContainerClient.listBlobsByHierarchy(bucketName + "/").stream()).hasSize(0);
    }

    @Override
    void createTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        String targetDirectory = bucketName + "/" + table;

        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(TestDeltaLakeAdlsConnectorSmokeTest.class.getClassLoader())
                    .getResources()
                    .stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath + "/"))
                    .collect(toImmutableList());
            for (ClassPath.ResourceInfo resourceInfo : resources) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(targetDirectory));
                ByteSource byteSource = resourceInfo.asByteSource();
                azureContainerClient.getBlobClient(fileName).upload(byteSource.openBufferedStream(), byteSource.size());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        queryRunner.execute(format("CREATE TABLE %s (dummy int) WITH (location = '%s')", table, getLocationForTable(bucketName, table)));
    }

    @Override
    String getLocationForTable(String bucketName, String tableName)
    {
        return fullAdlsUrl() + tableName;
    }

    @Override
    List<String> getTableFiles(String tableName)
    {
        return listAllFilesRecursive(tableName);
    }

    @Override
    protected List<String> listCheckpointFiles(String transactionLogDirectory)
    {
        return listAllFilesRecursive(transactionLogDirectory).stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .collect(toImmutableList());
    }

    private List<String> listAllFilesRecursive(String directory)
    {
        String azurePath = bucketName + "/" + directory + "/";
        Duration timeout = Duration.ofMinutes(5);
        List<String> allPaths = azureContainerClient.listBlobs(new ListBlobsOptions().setPrefix(azurePath), timeout).stream()
                .map(BlobItem::getName)
                .map(relativePath -> format("abfs://%s@%s.dfs.core.windows.net/%s", container, account, relativePath))
                .collect(toImmutableList());

        Set<String> directories = allPaths.stream()
                .map(path -> path.replaceFirst("/[^/]+$", ""))
                .collect(toImmutableSet());

        return allPaths.stream()
                .filter(path -> !path.endsWith("/") && !directories.contains(path))
                .collect(toImmutableList());
    }

    private String fullAdlsUrl()
    {
        return format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, bucketName);
    }
}
