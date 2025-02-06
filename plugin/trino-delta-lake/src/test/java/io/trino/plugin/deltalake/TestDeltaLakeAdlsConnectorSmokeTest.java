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

import com.azure.core.util.HttpClientOptions;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import com.google.common.reflect.ClassPath;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import reactor.netty.resources.ConnectionProvider;

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
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.filesystem.azure.AzureFileSystemFactory.createAzureHttpClient;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Matcher.quoteReplacement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testcontainers.containers.Network.newNetwork;

@TestInstance(PER_CLASS)
public class TestDeltaLakeAdlsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    private final String container;
    private final String account;
    private final String accessKey;
    private final String adlsDirectory;
    private BlobContainerClient azureContainerClient;

    public TestDeltaLakeAdlsConnectorSmokeTest()
    {
        this.container = requiredNonEmptySystemProperty("testing.azure-abfs-container");
        this.account = requiredNonEmptySystemProperty("testing.azure-abfs-account");
        this.accessKey = requiredNonEmptySystemProperty("testing.azure-abfs-access-key");
        this.adlsDirectory = format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, bucketName);
    }

    @Override
    protected HiveHadoop createHiveHadoop()
            throws Exception
    {
        String connectionString = format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net", account, accessKey);
        ConnectionProvider provider = ConnectionProvider.create("TestDeltaLakeAdsl");
        closeAfterClass(provider::dispose);

        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        closeAfterClass(eventLoopGroup::shutdownGracefully);

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString)
                .httpClient(createAzureHttpClient(provider, eventLoopGroup, new HttpClientOptions()))
                .buildClient();
        this.azureContainerClient = blobServiceClient.getBlobContainerClient(container);

        String abfsSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("io/trino/plugin/deltalake/hdp3.1-core-site.xml.abfs-template"), UTF_8)
                .replace("%ABFS_ACCESS_KEY%", accessKey)
                .replace("%ABFS_ACCOUNT%", account);

        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", posixFilePermissions);
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.writeString(hadoopCoreSiteXmlTempFile, abfsSpecificCoreSiteXmlContent);

        HiveHadoop hiveHadoop = HiveHadoop.builder()
                .withImage(HIVE3_IMAGE)
                .withNetwork(closeAfterClass(newNetwork()))
                .withFilesToMount(ImmutableMap.of("/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString()))
                .build();
        hiveHadoop.start();
        return hiveHadoop; // closed by superclass
    }

    @Override
    protected Map<String, String> hiveStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.hadoop.enabled", "false")
                .put("fs.native-azure.enabled", "true")
                .put("azure.auth-type", "ACCESS_KEY")
                .put("azure.access-key", accessKey)
                .buildOrThrow();
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return hiveStorageConfiguration();
    }

    @AfterAll
    public void removeTestData()
    {
        if (adlsDirectory != null) {
            hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-rm", "-f", "-r", adlsDirectory);
        }
        assertThat(azureContainerClient.listBlobsByHierarchy(bucketName + "/")).isEmpty();
    }

    @Override
    protected void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        String targetDirectory = bucketName + "/" + table;

        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(getClass().getClassLoader())
                    .getResources()
                    .stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath + "/"))
                    .collect(toImmutableList());
            for (ClassPath.ResourceInfo resourceInfo : resources) {
                String fileName = resourceInfo.getResourceName()
                        .replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(targetDirectory));
                ByteSource byteSource = resourceInfo.asByteSource();
                azureContainerClient.getBlobClient(fileName).upload(byteSource.openBufferedStream(), byteSource.size());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        queryRunner.execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", table, getLocationForTable(bucketName, table)));
    }

    @Override
    protected String getLocationForTable(String bucketName, String tableName)
    {
        return bucketUrl() + tableName;
    }

    @Override
    protected List<String> getTableFiles(String tableName)
    {
        return listAllFilesRecursive(tableName);
    }

    @Override
    protected List<String> listFiles(String directory)
    {
        return listAllFilesRecursive(directory).stream()
                .collect(toImmutableList());
    }

    private List<String> listAllFilesRecursive(String directory)
    {
        String azurePath = bucketName + "/" + directory;
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

    @Override
    protected void deleteFile(String filePath)
    {
        String blobName = bucketName + "/" + filePath.substring(bucketUrl().length());
        azureContainerClient.getBlobClient(blobName)
                .delete();
    }

    @Override
    protected String bucketUrl()
    {
        return format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, bucketName);
    }
}
