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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.reflect.ClassPath;
import io.airlift.log.Logger;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Matcher.quoteReplacement;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.testcontainers.containers.Network.newNetwork;

/**
 * This test requires these variables to connect to GCS:
 * - gcp-storage-bucket: The name of the bucket to store tables in. The bucket must already exist.
 * - gcp-credentials-key: A base64 encoded copy of the JSON authentication file for the service account used to connect to GCP.
 *   For example, `cat service-account-key.json | base64`
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDeltaLakeGcsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestDeltaLakeGcsConnectorSmokeTest.class);
    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));

    private final String gcpStorageBucket;
    private final String gcpCredentialKey;

    private Path gcpCredentialsFile;
    private String gcpCredentials;
    private TrinoFileSystem fileSystem;

    public TestDeltaLakeGcsConnectorSmokeTest()
    {
        this.gcpStorageBucket = requiredNonEmptySystemProperty("testing.gcp-storage-bucket");
        this.gcpCredentialKey = requiredNonEmptySystemProperty("testing.gcp-credentials-key");
    }

    @Override
    protected void environmentSetup()
    {
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        gcpCredentials = new String(jsonKeyBytes, UTF_8);
        try {
            this.gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json", READ_ONLY_PERMISSIONS);
            gcpCredentialsFile.toFile().deleteOnExit();
            Files.write(gcpCredentialsFile, jsonKeyBytes);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @AfterAll
    public void removeTestData()
    {
        if (fileSystem != null) {
            try {
                fileSystem.deleteDirectory(Location.of(bucketUrl()));
            }
            catch (IOException e) {
                // The GCS bucket should be configured to expire objects automatically. Clean up issues do not need to fail the test.
                LOG.warn(e, "Failed to clean up GCS test directory: %s", bucketUrl());
            }
            fileSystem = null;
        }
    }

    @Override
    protected HiveHadoop createHiveHadoop()
            throws Exception
    {
        String gcpSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("io/trino/plugin/deltalake/hdp3.1-core-site.xml.gcs-template"), UTF_8)
                .replace("%GCP_CREDENTIALS_FILE_PATH%", "/etc/hadoop/conf/gcp-credentials.json");

        Path hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", READ_ONLY_PERMISSIONS);
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.writeString(hadoopCoreSiteXmlTempFile, gcpSpecificCoreSiteXmlContent);

        HiveHadoop hiveHadoop = HiveHadoop.builder()
                .withImage(HIVE3_IMAGE)
                .withNetwork(closeAfterClass(newNetwork()))
                .withFilesToMount(ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString(),
                        "/etc/hadoop/conf/gcp-credentials.json", gcpCredentialsFile.toAbsolutePath().toString()))
                .build();
        hiveHadoop.start();
        return hiveHadoop; // closed by superclass
    }

    @Override
    protected Map<String, String> hiveStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.native-gcs.enabled", "true")
                .put("gcs.json-key", gcpCredentials)
                .buildOrThrow();
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .putAll(hiveStorageConfiguration())
                // TODO why not unique table locations? (This is here since 52bf6680c1b25516f6e8e64f82ada089abc0c9d3.)
                .put("delta.unique-table-location", "false")
                .buildOrThrow();
    }

    @Override
    protected void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        this.fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        String targetDirectory = bucketUrl() + table;

        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(getClass().getClassLoader())
                    .getResources()
                    .stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath + "/"))
                    .collect(toImmutableList());
            for (ClassPath.ResourceInfo resourceInfo : resources) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(targetDirectory));
                byte[] bytes = resourceInfo.asByteSource().read();
                TrinoOutputFile trinoOutputFile = fileSystem.newOutputFile(Location.of(fileName));
                trinoOutputFile.createOrOverwrite(bytes);
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
        ImmutableList.Builder<String> locations = ImmutableList.builder();
        try {
            FileIterator files = fileSystem.listFiles(Location.of(bucketUrl()).appendPath(directory));
            while (files.hasNext()) {
                locations.add(files.next().location().toString());
            }
            return locations.build();
        }
        catch (FileNotFoundException e) {
            return ImmutableList.of();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void deleteFile(String filePath)
    {
        try {
            fileSystem.deleteFile(Location.of(filePath));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String bucketUrl()
    {
        return format("gs://%s/%s/", gcpStorageBucket, bucketName);
    }
}
