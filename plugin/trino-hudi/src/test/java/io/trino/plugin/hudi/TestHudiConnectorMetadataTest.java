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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.filesystem.hdfs.HdfsFileSystemModule;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hudi.security.HudiAccessControlMetadataFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.TypeManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingConnectorContext;
import io.trino.testing.TestingConnectorSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.plugin.hive.HiveTableProperties.PARTITIONED_BY_PROPERTY;
import static io.trino.plugin.hive.HiveTableProperties.STORAGE_FORMAT_PROPERTY;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.hudi.BaseHudiConnectorTest.SCHEMA_NAME;
import static io.trino.plugin.hudi.HudiStorageFormat.HOODIE_PARQUET;
import static io.trino.plugin.hudi.HudiTableProperties.HUDI_TABLE_TYPE;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestHudiConnectorMetadataTest
{
    private HudiMetadata metadata;
    private HiveMetastore hiveMetastore;
    private ConnectorSession connectorSession;
    private java.nio.file.Path temporaryStagingDirectory;
    protected static final String TEMPORARY_TABLE_PREFIX = "tmp_trino_test_";
    private static final List<ColumnMetadata> CREATE_TABLE_COLUMNS = ImmutableList.<ColumnMetadata>builder()
            .add(new ColumnMetadata("id", BIGINT))
            .add(new ColumnMetadata("t_string", createUnboundedVarcharType()))
            .add(new ColumnMetadata("t_tinyint", TINYINT))
            .add(new ColumnMetadata("t_smallint", SMALLINT))
            .add(new ColumnMetadata("t_integer", INTEGER))
            .add(new ColumnMetadata("t_bigint", BIGINT))
            .add(new ColumnMetadata("t_double", DOUBLE))
            .add(new ColumnMetadata("t_boolean", BOOLEAN))
            .build();

    private static final Map<String, Object> tableProperties = ImmutableMap.<String, Object>builder()
            .put(STORAGE_FORMAT_PROPERTY, HOODIE_PARQUET)
            .put(PARTITIONED_BY_PROPERTY, ImmutableList.of("t_string", "t_tinyint"))
            .put(HUDI_TABLE_TYPE, HudiTableType.COPY_ON_WRITE.getName())
            .buildOrThrow();

    @BeforeClass
    public void init()
            throws Exception
    {
        temporaryStagingDirectory = createTempDirectory("trino-hudi-staging-");
        Map<String, String> config = ImmutableMap.of();

        Bootstrap app = new Bootstrap(
                // connector dependencies
                new JsonModule(),
                binder -> {
                    ConnectorContext context = new TestingConnectorContext();
                    binder.bind(CatalogName.class).toInstance(new CatalogName("test"));
                    binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                    binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    binder.bind(HudiAccessControlMetadataFactory.class).toInstance(HudiAccessControlMetadataFactory.SYSTEM);
                    binder.bind(ConnectorSession.class).toInstance(newSession(ImmutableMap.of()));
                },
                // connector modules
                new HudiModule(),
                // test setup
                binder -> {
                    binder.bind(HdfsEnvironment.class).toInstance(HDFS_ENVIRONMENT);
                    binder.install(new HdfsFileSystemModule());
                },
                new AbstractModule()
                {
                    @Provides
                    public Session getSession()
                    {
                        return testSessionBuilder()
                                .setCatalog("hudi")
                                .setSchema(SCHEMA_NAME)
                                .build();
                    }

                    @Provides
                    public DistributedQueryRunner getQueryRunner(Session session)
                            throws Exception
                    {
                        return DistributedQueryRunner
                                .builder(session)
                                .setExtraProperties(ImmutableMap.of())
                                .build();
                    }

                    @Provides
                    @Singleton
                    public HiveMetastore getHiveMetastore(DistributedQueryRunner queryRunner)
                    {
                        java.nio.file.Path coordinatorBaseDir = queryRunner.getCoordinator().getBaseDataDir();
                        File catalogDir = coordinatorBaseDir.resolve("catalog").toFile();
                        return createTestingFileHiveMetastore(catalogDir);
                    }

                    @Provides
                    public HiveMetastoreFactory getHiveMetastoreFactory(HiveMetastore metastore)
                    {
                        return HiveMetastoreFactory.ofInstance(metastore);
                    }

                    @Provides
                    public HudiMetadata getHudiMetadata(
                            HiveMetastore metastore,
                            HdfsEnvironment hdfsEnvironment,
                            TypeManager typeManager,
                            HudiAccessControlMetadataFactory accessControlMetadataFactory)
                    {
                        return new HudiMetadata(
                                metastore,
                                hdfsEnvironment,
                                typeManager,
                                accessControlMetadataFactory.create(metastore));
                    }
                });

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        metadata = injector.getInstance(HudiMetadata.class);
        hiveMetastore = injector.getInstance(HiveMetastore.class);
        connectorSession = injector.getInstance(ConnectorSession.class);
        Database database = Database.builder()
                .setDatabaseName(SCHEMA_NAME)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        try {
            hiveMetastore.createDatabase(database);
        }
        catch (SchemaAlreadyExistsException e) {
            // do nothing if database already exists
        }
    }

    protected static Map<String, Object> createTableProperties(HudiStorageFormat storageFormat)
    {
        return createTableProperties(storageFormat, ImmutableList.of(), HudiTableType.COPY_ON_WRITE);
    }

    protected static Map<String, Object> createTableProperties(HudiStorageFormat storageFormat, Iterable<String> parititonedBy, HudiTableType tableType)
    {
        return ImmutableMap.<String, Object>builder()
                .put(STORAGE_FORMAT_PROPERTY, storageFormat)
                .put(PARTITIONED_BY_PROPERTY, ImmutableList.copyOf(parititonedBy))
                .put(HUDI_TABLE_TYPE, tableType.getName())
                .buildOrThrow();
    }

    protected SchemaTableName temporaryTable(String tableName)
    {
        return temporaryTable(SCHEMA_NAME, tableName);
    }

    protected static SchemaTableName temporaryTable(String database, String tableName)
    {
        String randomName = UUID.randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        return new SchemaTableName(database, TEMPORARY_TABLE_PREFIX + tableName + "_" + randomName);
    }

    protected ConnectorSession newSession(Map<String, Object> propertyValues)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(getHiveConfig()).getSessionProperties())
                .setPropertyValues(propertyValues)
                .build();
    }

    protected HiveConfig getHiveConfig()
    {
        return new HiveConfig()
                .setMaxOpenSortFiles(10)
                .setTemporaryStagingDirectoryPath(temporaryStagingDirectory.toAbsolutePath().toString())
                .setWriterSortBufferSize(DataSize.of(100, KILOBYTE));
    }

    @Test
    public void testDefault()
    {
        SchemaTableName temporaryCreateTable = temporaryTable("create_hudi_table");
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateTable, CREATE_TABLE_COLUMNS, createTableProperties(HOODIE_PARQUET));
        HudiOutputTableHandle outputHandle = metadata.beginCreateTable(connectorSession, tableMetadata, Optional.empty(), NO_RETRIES);
        assertEquals(outputHandle.getTableName(), temporaryCreateTable.getTableName());
        assertEquals(outputHandle.getSchemaName(), temporaryCreateTable.getSchemaName());
        assertEquals(outputHandle.getTableType(), HudiTableType.COPY_ON_WRITE);
    }

    @Test
    public void testCreateWithPartition()
    {
        SchemaTableName temporaryCreateTable = temporaryTable("create_hudi_partitioned_table");
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(temporaryCreateTable, CREATE_TABLE_COLUMNS, tableProperties);
        HudiOutputTableHandle outputHandle = metadata.beginCreateTable(connectorSession, tableMetadata, Optional.empty(), NO_RETRIES);
        assertEquals(outputHandle.getTableName(), temporaryCreateTable.getTableName());
        assertEquals(outputHandle.getSchemaName(), temporaryCreateTable.getSchemaName());
        assertEquals(outputHandle.getTableType(), HudiTableType.COPY_ON_WRITE);
    }
}
