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

import com.google.cloud.NoCredentials;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.storage.BucketInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.reflect.ClassPath;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.gcs.GcsAuth;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.FlociGcp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.Network;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.regex.Matcher.quoteReplacement;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;
import static org.testcontainers.containers.Network.newNetwork;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDeltaLakeGcsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestDeltaLakeGcsConnectorSmokeTest.class);
    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
    private static final String HADOOP_GCS_CONNECTOR = "/usr/hdp/3.1.0.0-78/hadoop-mapreduce/gcs-connector-1.9.10.3.1.0.0-78-shaded.jar";
    private static final String TEZ_GCS_CONNECTOR = "/usr/hdp/3.1.0.0-78/tez/lib/gcs-connector-1.9.10.3.1.0.0-78-shaded.jar";

    private FlociGcp flociGcp;
    private Network network;
    private TrinoFileSystem fileSystem;

    @Override
    protected void environmentSetup()
    {
        network = closeAfterClass(newNetwork());
        flociGcp = closeAfterClass(new FlociGcp().withNetwork(network));
        flociGcp.start();

        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setEndpoint(Optional.of(flociGcp.getEndpoint().toString()))
                .setProjectId(FLOCI_GCP_PROJECT_ID);
        GcsStorageFactory storageFactory = new GcsStorageFactory(
                config,
                (builder, _) -> builder.setCredentials(NoCredentials.getInstance()));
        storageFactory.create(ConnectorIdentity.ofUser("test")).create(BucketInfo.of(bucketName));
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
        Path containerCoreSite = createCoreSite(flociGcp.getContainerEndpoint().toString());

        String gcsConnector = Path.of(GoogleHadoopFileSystem.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .toAbsolutePath()
                .toString();
        HiveHadoop hiveHadoop = HiveHadoop.builder()
                .withImage(HIVE3_IMAGE)
                .withNetwork(network)
                .withFilesToMount(ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", containerCoreSite.normalize().toAbsolutePath().toString(),
                        HADOOP_GCS_CONNECTOR, gcsConnector,
                        TEZ_GCS_CONNECTOR, gcsConnector))
                .build();
        hiveHadoop.start();
        return hiveHadoop; // closed by superclass
    }

    private static Path createCoreSite(String endpoint)
            throws IOException
    {
        String content = Resources.toString(Resources.getResource("io/trino/plugin/deltalake/hdp3.1-core-site.xml.gcs-template"), UTF_8)
                .replace("%GCS_ENDPOINT%", endpoint + "/")
                .replace("%GCP_PROJECT_ID%", FLOCI_GCP_PROJECT_ID);
        Path coreSite = Files.createTempFile("core-site", ".xml", READ_ONLY_PERMISSIONS);
        coreSite.toFile().deleteOnExit();
        Files.writeString(coreSite, content);
        return coreSite;
    }

    @Override
    protected Map<String, String> hiveStorageConfiguration()
    {
        return ImmutableMap.of(
                "gcs.endpoint", flociGcp.getEndpoint().toString(),
                "gcs.project-id", FLOCI_GCP_PROJECT_ID);
    }

    @Override
    protected Map<String, String> deltaStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("gcs.endpoint", flociGcp.getEndpoint().toString())
                .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                // TODO why not unique table locations? (This is here since 52bf6680c1b25516f6e8e64f82ada089abc0c9d3.)
                .put("delta.unique-table-location", "false")
                .buildOrThrow();
    }

    @Override
    protected Module additionalDeltaLakeModule()
    {
        return gcsModule();
    }

    @Override
    protected Module additionalHiveModule()
    {
        return gcsModule();
    }

    private static Module gcsModule()
    {
        return binder -> {
            configBinder(binder).bindConfig(GcsFileSystemConfig.class);
            binder.bind(GcsAuth.class)
                    .toInstance((builder, _) -> builder.setCredentials(NoCredentials.getInstance()));
            binder.bind(GcsStorageFactory.class).in(Scopes.SINGLETON);
            binder.bind(GcsFileSystemFactory.class).in(Scopes.SINGLETON);
            newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                    .addBinding("gs")
                    .to(GcsFileSystemFactory.class);
        };
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

    @Test
    @Override
    @Disabled("Floci GCP does not preserve URI-encoded object names")
    public void testPathUriDecoding() {}

    @Test
    @Override
    @Disabled("Floci GCP does not preserve URI-encoded object names")
    public void testPathUriEncoding() {}

    @Test
    @Override
    @Disabled("Floci GCP does not preserve URI-encoded object names")
    public void testVacuumWithWhiteSpace() {}

    @RepeatedTest(3)
    @Override
    @Disabled("Floci GCP does not atomically enforce object preconditions")
    public void testConcurrentInsertsReconciliationForBlindInserts() {}

    @RepeatedTest(3)
    @Override
    @Disabled("Floci GCP does not atomically enforce object preconditions")
    public void testConcurrentDeletePushdownReconciliation() {}

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
        return format("gs://%s/%s/", bucketName, bucketName);
    }
}
