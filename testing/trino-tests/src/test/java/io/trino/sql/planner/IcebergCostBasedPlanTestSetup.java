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

package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Table;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.containers.Minio;
import io.trino.testing.containers.junit.ReportLeakedContainers;
import io.trino.testing.minio.MinioClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.iceberg.CatalogType.TESTING_FILE_METASTORE;
import static io.trino.plugin.iceberg.IcebergConfig.EXTENDED_STATISTICS_CONFIG;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FILE_EXTENSION;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public class IcebergCostBasedPlanTestSetup
{
    private static final Logger log = Logger.get(IcebergCostBasedPlanTestSetup.class);

    // Iceberg metadata files are linked using absolute paths, so the bucket name must match where the metadata was exported from.
    // See more at https://github.com/apache/iceberg/issues/1617
    private static final String BUCKET_NAME = "starburst-benchmarks-data";

    // The container needs to be shared, since bucket name cannot be reused between tests.
    // The bucket name is used as a key in TrinoFileSystemCache which is managed in static manner.
    @GuardedBy("sharedMinioLock")
    private static Minio sharedMinio;
    @GuardedBy("sharedMinioLock")
    private static boolean sharedMinioClosed;
    private static final Object sharedMinioLock = new Object();

    private Minio minio;
    private Path temporaryMetastoreDirectory;
    private HiveMetastore hiveMetastore;
    private Map<String, String> connectorConfiguration;

    public ConnectorFactory createConnectorFactory()
    {
        synchronized (sharedMinioLock) {
            if (sharedMinio == null) {
                checkState(!sharedMinioClosed, "sharedMinio already closed");
                Minio minio = Minio.builder().build();
                minio.start();
                minio.createBucket(BUCKET_NAME);
                sharedMinio = minio;
                Runtime.getRuntime().addShutdownHook(new Thread(IcebergCostBasedPlanTestSetup::disposeSharedResources));
                // Disable ReportLeakedContainers for this container, as it is intentional that it stays after tests finish
                ReportLeakedContainers.ignoreContainerId(sharedMinio.getContainerId());
            }
            minio = sharedMinio;
        }

        try {
            temporaryMetastoreDirectory = createTempDirectory("file-metastore");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        connectorConfiguration = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", TESTING_FILE_METASTORE.name())
                .put("hive.metastore.catalog.dir", temporaryMetastoreDirectory.toString())
                .put("fs.native-s3.enabled", "true")
                .put("fs.hadoop.enabled", "true")
                .put("s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("s3.region", MINIO_REGION)
                .put("s3.endpoint", minio.getMinioAddress())
                .put("s3.path-style-access", "true")
                .put(EXTENDED_STATISTICS_CONFIG, "true")
                .put("bootstrap.quiet", "true")
                .buildOrThrow();

        return new IcebergConnectorFactory()
        {
            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                checkArgument(config.isEmpty(), "Unexpected configuration %s", config);
                Connector connector = super.create(catalogName, connectorConfiguration, context);
                hiveMetastore = ((IcebergConnector) connector).getInjector()
                        .getInstance(HiveMetastoreFactory.class)
                        .createMetastore(Optional.empty());
                return connector;
            }
        };
    }

    public void createDatabase(String schema)
    {
        hiveMetastore.createDatabase(
                Database.builder()
                        .setDatabaseName(schema)
                        .setOwnerName(Optional.empty())
                        .setOwnerType(Optional.empty())
                        .build());
    }

    // Iceberg metadata files are linked using absolute paths, so the path within the bucket name must match where the metadata was exported from.
    public void populateTablesFromResource(List<String> tableNames, String schema, String resourceDirectory, String targetDirectory)
    {
        for (String tableName : tableNames) {
            String resourcePath = resourceDirectory + tableName;
            String targetPath = targetDirectory + tableName;
            log.info("Copying resources for %s table from %s to %s in the container", tableName, resourcePath, targetPath);
            minio.copyResources(resourcePath, BUCKET_NAME, targetPath);

            String tableLocation = "s3://%s/%s".formatted(BUCKET_NAME, targetPath);
            String metadataLocation;
            try (MinioClient minioClient = minio.createMinioClient()) {
                String metadataPath = minioClient.listObjects(BUCKET_NAME, targetPath + "/metadata/").stream()
                        .filter(path -> path.endsWith(METADATA_FILE_EXTENSION))
                        .collect(onlyElement());
                metadataLocation = "s3://%s/%s".formatted(BUCKET_NAME, metadataPath);
            }

            log.info("Registering table %s using metadata location %s", tableName, metadataLocation);
            hiveMetastore.createTable(
                    Table.builder()
                            .setDatabaseName(schema)
                            .setTableName(tableName)
                            .setOwner(Optional.empty())
                            .setTableType(EXTERNAL_TABLE.name())
                            .setDataColumns(List.of())
                            .withStorage(storage -> storage.setLocation(tableLocation))
                            .withStorage(storage -> storage.setStorageFormat(ICEBERG_METASTORE_STORAGE_FORMAT))
                            // This is a must-have property for the EXTERNAL_TABLE table type
                            .setParameter("EXTERNAL", "TRUE")
                            .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                            .setParameter(METADATA_LOCATION_PROP, metadataLocation)
                            .build(),
                    NO_PRIVILEGES);
        }
    }

    public void cleanUp()
            throws Exception
    {
        if (minio != null) {
            // Don't stop container, as it's shared
            synchronized (sharedMinioLock) {
                verify(minio == sharedMinio);
            }
            minio = null;
        }

        if (temporaryMetastoreDirectory != null) {
            deleteRecursively(temporaryMetastoreDirectory, ALLOW_INSECURE);
        }

        hiveMetastore = null;
        connectorConfiguration = null;
    }

    private static void disposeSharedResources()
    {
        synchronized (sharedMinioLock) {
            sharedMinioClosed = true;
            if (sharedMinio != null) {
                sharedMinio.stop();
                sharedMinio = null;
            }
        }
    }
}
