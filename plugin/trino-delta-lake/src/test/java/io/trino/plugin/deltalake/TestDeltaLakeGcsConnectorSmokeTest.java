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
import com.google.common.reflect.ClassPath;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.spi.NodeVersion;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.FlociGcp;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.regex.Matcher.quoteReplacement;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDeltaLakeGcsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    private String endpoint;
    private String serviceAccountJson;
    private TrinoFileSystem fileSystem;

    @Override
    protected void environmentSetup()
    {
        FlociGcp flociGcp = closeAfterClass(new FlociGcp());
        flociGcp.start();
        flociGcp.createBucket(bucketName);
        endpoint = flociGcp.getEndpoint().toString();
        serviceAccountJson = flociGcp.getServiceAccountJson();
    }

    @Override
    protected HiveMetastore createMetastore()
            throws IOException
    {
        Path metastoreDirectory = createTempDirectory("delta-gcs-metastore");
        closeAfterClass(() -> deleteRecursively(metastoreDirectory, ALLOW_INSECURE));
        return new GcsTestingFileHiveMetastore(metastoreDirectory);
    }

    @Override
    protected boolean supportsManagedTableRename()
    {
        return true;
    }

    @Override
    protected Map<String, String> hiveStorageConfiguration()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.gcs.enabled", "true")
                .put("gcs.auth-type", "SERVICE_ACCOUNT")
                .put("gcs.endpoint", endpoint)
                .put("gcs.json-key", serviceAccountJson)
                .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
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
        if (fileSystem == null) {
            fileSystem = getConnectorService(queryRunner, TrinoFileSystemFactory.class)
                    .create(ConnectorIdentity.ofUser("test"));
        }

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
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected String bucketUrl()
    {
        return format("gs://%s/", bucketName);
    }

    private final class GcsTestingFileHiveMetastore
            extends FileHiveMetastore
    {
        private GcsTestingFileHiveMetastore(Path metastoreDirectory)
        {
            super(new NodeVersion("testversion"),
                    new LocalFileSystemFactory(metastoreDirectory),
                    new HiveMetastoreConfig().isHideDeltaLakeTables(),
                    new FileHiveMetastoreConfig()
                            .setCatalogDirectory("local:///")
                            .setDisableLocationChecks(true)
                            .setMetastoreUser("test"));
        }

        @Override
        public void createDatabase(Database database)
        {
            super.createDatabase(Database.builder(database)
                    .setLocation(database.getLocation().or(() -> Optional.of(bucketUrl() + database.getDatabaseName())))
                    .build());
        }
    }
}
