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
import io.airlift.units.Duration;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.metastore.Database;
import io.trino.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.containers.HiveFlociDataLake;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergSessionProperties;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.IntegerType;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreApiDatabase;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestTrinoHiveCatalogViewRedirection
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private HiveFlociDataLake dataLake;
    private CachingHiveMetastore metastore;
    private TrinoCatalog catalog;
    private String namespace;
    private String bucketName;

    @BeforeAll
    public void setUp()
    {
        bucketName = "test-view-redirection-" + randomNameSuffix();
        dataLake = closer.register(new Hive3FlociDataLake(bucketName, HIVE3_IMAGE));
        dataLake.start();

        TrinoFileSystemFactory fileSystemFactory = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setEndpoint(dataLake.floci().endpoint().toString())
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY)
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true),
                new S3FileSystemStats());
        ThriftMetastore thriftMetastore = testingThriftHiveMetastoreBuilder()
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        .setReadTimeout(new Duration(1, MINUTES)))
                .metastoreClient(dataLake.getHiveMetastoreEndpoint())
                .build(closer::register);
        metastore = createPerTransactionCache(new BridgingHiveMetastore(thriftMetastore), 1000);

        catalog = new TrinoHiveCatalog(
                new CatalogName("catalog"),
                metastore,
                new TrinoViewHiveMetastore(metastore, false, "trino-version", "Test"),
                fileSystemFactory,
                FILE_IO_FACTORY,
                TESTING_TYPE_MANAGER,
                new HiveMetastoreTableOperationsProvider(
                        fileSystemFactory,
                        FILE_IO_FACTORY,
                        new ThriftMetastoreFactory()
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
                        },
                        new IcebergHiveCatalogConfig()),
                false,
                false,
                false,
                true,
                directExecutor(),
                newDirectExecutorService());

        namespace = "test_view_redirect_" + randomNameSuffix();
        thriftMetastore.createDatabase(toMetastoreApiDatabase(Database.builder()
                .setDatabaseName(namespace)
                .setOwnerName(Optional.of("test"))
                .setOwnerType(Optional.of(PrincipalType.USER))
                .setLocation(Optional.of("s3://%s/%s".formatted(bucketName, namespace)))
                .build()));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        try {
            catalog.dropNamespace(SESSION, namespace);
        }
        catch (Exception _) {
        }
        closer.close();
    }

    @Test
    public void testRedirectViewWithNullParameters()
    {
        SchemaTableName viewName = new SchemaTableName(namespace, "test_view");
        assertThatThrownBy(() -> catalog.redirectView(null, viewName, "hive"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> catalog.redirectView(SESSION, null, "hive"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("viewName is null");
        assertThatThrownBy(() -> catalog.redirectView(SESSION, viewName, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("hiveCatalogName is null");
    }

    @Test
    public void testRedirectViewWithSystemSchema()
    {
        assertThat(catalog.redirectView(SESSION, new SchemaTableName("information_schema", "views"), "hive"))
                .isEmpty();
    }

    @Test
    public void testRedirectViewWithNonExistentView()
    {
        // View doesn't exist in metastore at all → no redirect (empty from metastore lookup)
        SchemaTableName viewName = new SchemaTableName(namespace, "nonexistent_view_" + randomNameSuffix());
        Optional<CatalogSchemaTableName> result = catalog.redirectView(SESSION, viewName, "hive");
        assertThat(result).isEmpty();
    }

    @Test
    public void testRedirectViewWithExistingPrestoView()
    {
        // Create a Presto/Trino view in the metastore
        String viewSuffix = randomNameSuffix();
        SchemaTableName viewName = new SchemaTableName(namespace, "presto_view_" + viewSuffix);

        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT 1 AS col1",
                Optional.of("catalog"),
                Optional.of(namespace),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("col1", IntegerType.INTEGER.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());
        catalog.createView(SESSION, viewName, viewDefinition, ImmutableMap.of(), false);

        try {
            // This is a Presto view (isSomeKindOfAView == true) → should redirect to hive catalog
            Optional<CatalogSchemaTableName> result = catalog.redirectView(SESSION, viewName, "hive");
            assertThat(result).isPresent();
            assertThat(result.get().getCatalogName()).isEqualTo("hive");
            assertThat(result.get().getSchemaTableName()).isEqualTo(viewName);
        }
        finally {
            catalog.dropView(SESSION, viewName);
        }
    }

    @Test
    public void testRedirectViewWithDifferentHiveCatalogNames()
    {
        String viewSuffix = randomNameSuffix();
        SchemaTableName viewName = new SchemaTableName(namespace, "view_catalog_names_" + viewSuffix);

        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT 1 AS col1",
                Optional.of("catalog"),
                Optional.of(namespace),
                ImmutableList.of(new ConnectorViewDefinition.ViewColumn("col1", IntegerType.INTEGER.getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());
        catalog.createView(SESSION, viewName, viewDefinition, ImmutableMap.of(), false);

        try {
            for (String hiveCatalogName : ImmutableList.of("hive", "hive_catalog", "my_hive", "legacy_hive")) {
                Optional<CatalogSchemaTableName> result = catalog.redirectView(SESSION, viewName, hiveCatalogName);
                assertThat(result).isPresent();
                assertThat(result.get().getCatalogName()).isEqualTo(hiveCatalogName);
                assertThat(result.get().getSchemaTableName()).isEqualTo(viewName);
            }
        }
        finally {
            catalog.dropView(SESSION, viewName);
        }
    }

    @Test
    public void testRedirectViewWithMaterializedView()
    {
        // Create a materialized view — should NOT be redirected by redirectView
        String mvSuffix = randomNameSuffix();
        SchemaTableName mvName = new SchemaTableName(namespace, "mv_no_redirect_" + mvSuffix);

        catalog.createMaterializedView(
                SESSION,
                mvName,
                new ConnectorMaterializedViewDefinition(
                        "SELECT 1 AS col1",
                        Optional.empty(),
                        Optional.of("catalog"),
                        Optional.of(namespace),
                        ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("col1", IntegerType.INTEGER.getTypeId(), Optional.empty())),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of()),
                Map.of(FILE_FORMAT_PROPERTY, PARQUET, FORMAT_VERSION_PROPERTY, 1),
                false,
                false);

        try {
            Optional<CatalogSchemaTableName> result = catalog.redirectView(SESSION, mvName, "hive");
            assertThat(result)
                    .describedAs("Materialized views should not be redirected by redirectView")
                    .isEmpty();
        }
        finally {
            catalog.dropMaterializedView(SESSION, mvName);
        }
    }

    @Test
    public void testRedirectViewWithRegularTable()
    {
        // A regular Iceberg table should NOT be redirected by redirectView
        SchemaTableName tableName = new SchemaTableName(namespace, "regular_table_" + randomNameSuffix());

        catalog.newCreateTableTransaction(
                        SESSION,
                        tableName,
                        new Schema(
                                Types.NestedField.optional(1, "col1", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        Optional.of("s3://" + bucketName + "/" + namespace + "/" + tableName.getTableName()),
                        ImmutableMap.of())
                .commitTransaction();

        try {
            Optional<CatalogSchemaTableName> result = catalog.redirectView(SESSION, tableName, "hive");
            assertThat(result)
                    .describedAs("Regular tables should not be redirected by redirectView")
                    .isEmpty();
        }
        finally {
            catalog.dropTable(SESSION, tableName);
        }
    }
}
