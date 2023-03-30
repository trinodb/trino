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
import io.airlift.log.Logger;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.iceberg.IcebergConnectorFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import io.trino.testng.services.ManageTestResources;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.iceberg.CatalogType.TESTING_FILE_METASTORE;
import static io.trino.plugin.iceberg.IcebergConfig.EXTENDED_STATISTICS_CONFIG;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FILE_EXTENSION;
import static io.trino.plugin.iceberg.catalog.AbstractIcebergTableOperations.ICEBERG_METASTORE_STORAGE_FORMAT;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public abstract class BaseIcebergCostBasedPlanTest
        extends BaseCostBasedPlanTest
{
    private static final Logger log = Logger.get(BaseIcebergCostBasedPlanTest.class);

    // Iceberg metadata files are linked using absolute paths, so the bucket name must match where the metadata was exported from.
    // See more at https://github.com/apache/iceberg/issues/1617
    private static final String BUCKET_NAME = "starburst-benchmarks-data";

    // The container needs to be shared, since bucket name cannot be reused between tests.
    // The bucket name is used as a key in TrinoFileSystemCache which is managed in static manner.
    @GuardedBy("sharedMinioLock")
    @ManageTestResources.Suppress(because = "This resource is leaked, but consciously -- there is no known way to avoid that")
    private static Minio sharedMinio;
    private static final Object sharedMinioLock = new Object();

    protected Minio minio;
    private Path temporaryMetastoreDirectory;
    private FileHiveMetastore fileMetastore;
    private Map<String, String> connectorConfiguration;

    protected BaseIcebergCostBasedPlanTest(String schemaName, String fileFormatName, boolean partitioned)
    {
        super(schemaName, Optional.of(fileFormatName), partitioned);
    }

    protected BaseIcebergCostBasedPlanTest(String schemaName, String fileFormatName, boolean partitioned, boolean smallFiles)
    {
        super(schemaName, Optional.of(fileFormatName), partitioned, smallFiles);
    }

    @Override
    protected ConnectorFactory createConnectorFactory()
    {
        synchronized (sharedMinioLock) {
            if (sharedMinio == null) {
                Minio minio = Minio.builder().build();
                minio.start();
                minio.createBucket(BUCKET_NAME);
                sharedMinio = minio;
            }
            minio = sharedMinio;
        }

        try {
            temporaryMetastoreDirectory = createTempDirectory("file-metastore");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        HdfsConfig hdfsConfig = new HdfsConfig();
        HiveS3Config s3Config = new HiveS3Config()
                .setS3Endpoint(minio.getMinioAddress())
                .setS3AwsAccessKey(MINIO_ACCESS_KEY)
                .setS3AwsSecretKey(MINIO_SECRET_KEY)
                .setS3PathStyleAccess(true);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(
                new DynamicHdfsConfiguration(
                        new HdfsConfigurationInitializer(hdfsConfig, Set.of(new TrinoS3ConfigurationInitializer(s3Config))),
                        Set.of()),
                hdfsConfig,
                new NoHdfsAuthentication());

        fileMetastore = new FileHiveMetastore(
                // Must match the version picked by the LocalQueryRunner
                new NodeVersion("<unknown>"),
                hdfsEnvironment,
                false,
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(temporaryMetastoreDirectory.toString()));

        connectorConfiguration = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", TESTING_FILE_METASTORE.name())
                .put("hive.metastore.catalog.dir", temporaryMetastoreDirectory.toString())
                .put("hive.s3.endpoint", minio.getMinioAddress())
                .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                .put("hive.s3.path-style-access", "true")
                .put(EXTENDED_STATISTICS_CONFIG, "true")
                .buildOrThrow();

        return new IcebergConnectorFactory()
        {
            @Override
            public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
            {
                checkArgument(config.isEmpty(), "Unexpected configuration %s", config);
                return super.create(catalogName, connectorConfiguration, context);
            }
        };
    }

    @Override
    @BeforeClass
    public void prepareTables()
    {
        String schema = getQueryRunner().getDefaultSession().getSchema().orElseThrow();
        fileMetastore.createDatabase(
                Database.builder()
                        .setDatabaseName(schema)
                        .setOwnerName(Optional.empty())
                        .setOwnerType(Optional.empty())
                        .build());
        doPrepareTables();
    }

    protected abstract void doPrepareTables();

    // Iceberg metadata files are linked using absolute paths, so the path within the bucket name must match where the metadata was exported from.
    protected void populateTableFromResource(String tableName, String resourcePath, String targetPath)
    {
        String schema = getQueryRunner().getDefaultSession().getSchema().orElseThrow();

        log.info("Copying resources for %s unpartitioned table from %s to %s in the container", tableName, resourcePath, targetPath);
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
        fileMetastore.createTable(
                Table.builder()
                        .setDatabaseName(schema)
                        .setTableName(tableName)
                        .setOwner(Optional.empty())
                        .setTableType(TableType.EXTERNAL_TABLE.name())
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

    @AfterClass(alwaysRun = true)
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

        fileMetastore = null;
        connectorConfiguration = null;
    }

    @AfterSuite(alwaysRun = true)
    public static void disposeSharedResources()
    {
        synchronized (sharedMinioLock) {
            if (sharedMinio != null) {
                sharedMinio.stop();
                sharedMinio = null;
            }
        }
    }
}
