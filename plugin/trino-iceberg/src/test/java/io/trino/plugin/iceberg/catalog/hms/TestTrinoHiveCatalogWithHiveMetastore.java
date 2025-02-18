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
package io.trino.plugin.iceberg.catalog.hms;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.iceberg.IcebergSchemaProperties;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TestingTypeManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestTrinoHiveCatalogWithHiveMetastore
        extends BaseTrinoCatalogTest
{
    private static final Logger LOG = Logger.get(TestTrinoHiveCatalogWithHiveMetastore.class);

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    // Use MinIO for storage, since HDFS is hard to get working in a unit test
    private HiveMinioDataLake dataLake;
    private TrinoFileSystem fileSystem;
    private CachingHiveMetastore metastore;
    protected String bucketName;

    HiveMinioDataLake hiveMinioDataLake()
    {
        return new Hive3MinioDataLake(bucketName, HIVE3_IMAGE);
    }

    @BeforeAll
    public void setUp()
    {
        bucketName = "test-hive-catalog-with-hms-" + randomNameSuffix();
        dataLake = closer.register(hiveMinioDataLake());
        dataLake.start();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        dataLake = null;
        closer.close();
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        TrinoFileSystemFactory fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(dataLake.getMinio().getMinioAddress())
                        .setAwsAccessKey(MINIO_ACCESS_KEY)
                        .setAwsSecretKey(MINIO_SECRET_KEY)
                        .setRegion(MINIO_REGION)
                        .setPathStyleAccess(true),
                new S3FileSystemStats());
        ThriftMetastore thriftMetastore = testingThriftHiveMetastoreBuilder()
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        // Read timed out sometimes happens with the default timeout
                        .setReadTimeout(new Duration(1, MINUTES)))
                .metastoreClient(dataLake.getHiveMetastoreEndpoint())
                .build(closer::register);
        metastore = createPerTransactionCache(new BridgingHiveMetastore(thriftMetastore), 1000);
        fileSystem = fileSystemFactory.create(SESSION);

        return new TrinoHiveCatalog(
                new CatalogName("catalog"),
                metastore,
                new TrinoViewHiveMetastore(metastore, false, "trino-version", "Test"),
                fileSystemFactory,
                new TestingTypeManager(),
                new HiveMetastoreTableOperationsProvider(fileSystemFactory, new ThriftMetastoreFactory()
                {
                    @Override
                    public boolean isImpersonationEnabled()
                    {
                        verify(new ThriftMetastoreConfig().isImpersonationEnabled(), "This test wants to test the default behavior and assumes it's off");
                        return false;
                    }

                    @Override
                    public ThriftMetastore createMetastore(Optional<ConnectorIdentity> identity)
                    {
                        return thriftMetastore;
                    }
                }),
                useUniqueTableLocations,
                false,
                false,
                isHideMaterializedViewStorageTable(),
                directExecutor());
    }

    protected boolean isHideMaterializedViewStorageTable()
    {
        return true;
    }

    @Test
    public void testCreateMaterializedView()
            throws IOException
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_mv_" + randomNameSuffix();
        String materializedViewName = "materialized_view_name";
        try {
            catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
            catalog.createMaterializedView(
                    SESSION,
                    new SchemaTableName(namespace, materializedViewName),
                    new ConnectorMaterializedViewDefinition(
                            "SELECT * FROM tpch.tiny.nation",
                            Optional.empty(),
                            Optional.of("catalog_name"),
                            Optional.of("schema_name"),
                            ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("col1", INTEGER.getTypeId(), Optional.empty())),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableList.of()),
                    ImmutableMap.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1),
                    false,
                    false);
            List<SchemaTableName> materializedViews = catalog.listTables(SESSION, Optional.of(namespace)).stream()
                    .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                    .map(TableInfo::tableName)
                    .toList();
            assertThat(materializedViews).hasSize(1);
            assertThat(materializedViews.getFirst().getTableName()).isEqualTo(materializedViewName);
            Optional<ConnectorMaterializedViewDefinition> materializedView = catalog.getMaterializedView(SESSION, materializedViews.getFirst());
            assertThat(materializedView).isPresent();
            String storageTableName;
            if (isHideMaterializedViewStorageTable()) {
                storageTableName = materializedViewName;
            }
            else {
                Optional<CatalogSchemaTableName> storageTable = materializedView.get().getStorageTable();
                assertThat(storageTable).isPresent();
                storageTableName = storageTable.get().getSchemaTableName().getTableName();
            }
            Location dataLocation = Location.of(getNamespaceLocation(namespace) + "/" + storageTableName);
            assertThat(fileSystem.directoryExists(dataLocation).orElseThrow())
                    .describedAs("The directory corresponding to the table data for materialized view must exist")
                    .isTrue();
            catalog.dropMaterializedView(SESSION, new SchemaTableName(namespace, materializedViewName));
            assertThat(fileSystem.directoryExists(dataLocation))
                    .describedAs("The materialized view drop should also delete the data files associated with it")
                    .isEmpty();
        }
        finally {
            try {
                catalog.dropNamespace(SESSION, namespace);
            }
            catch (Exception e) {
                LOG.warn("Failed to clean up namespace: %s", namespace);
            }
        }
    }

    @Override
    protected Optional<SchemaTableName> createExternalIcebergTable(TrinoCatalog catalog, String namespace, AutoCloseableCloser closer)
            throws Exception
    {
        // simulate iceberg table created by spark with lowercase table type
        return createTableWithTableType(catalog, namespace, closer, "lowercase_type", Optional.of(ICEBERG_TABLE_TYPE_VALUE.toLowerCase(ENGLISH)));
    }

    @Override
    protected Optional<SchemaTableName> createExternalNonIcebergTable(TrinoCatalog catalog, String namespace, AutoCloseableCloser closer)
            throws Exception
    {
        return createTableWithTableType(catalog, namespace, closer, "non_iceberg_table", Optional.empty());
    }

    private Optional<SchemaTableName> createTableWithTableType(TrinoCatalog catalog, String namespace, AutoCloseableCloser closer, String tableName, Optional<String> tableType)
            throws Exception
    {
        SchemaTableName lowerCaseTableTypeTable = new SchemaTableName(namespace, tableName);
        catalog.newCreateTableTransaction(
                        SESSION,
                        lowerCaseTableTypeTable,
                        new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.of(arbitraryTableLocation(catalog, SESSION, lowerCaseTableTypeTable)),
                        ImmutableMap.of())
                .commitTransaction();

        Table metastoreTable = metastore.getTable(namespace, tableName).get();

        metastore.replaceTable(
                namespace,
                tableName,
                Table.builder(metastoreTable)
                        .setParameter(TABLE_TYPE_PROP, tableType)
                        .build(),
                NO_PRIVILEGES);
        closer.register(() -> metastore.dropTable(namespace, tableName, true));
        return Optional.of(lowerCaseTableTypeTable);
    }

    @Override
    protected Map<String, Object> defaultNamespaceProperties(String namespaceName)
    {
        return Map.of(IcebergSchemaProperties.LOCATION_PROPERTY, getNamespaceLocation(namespaceName));
    }

    private String getNamespaceLocation(String namespaceName)
    {
        return "s3://%s/%s".formatted(bucketName, namespaceName);
    }
}
