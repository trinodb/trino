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
package org.apache.iceberg.snowflake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.CommitTaskData;
import io.trino.plugin.iceberg.IcebergMetadata;
import io.trino.plugin.iceberg.TableStatisticsWriter;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.snowflake.IcebergSnowflakeCatalogConfig;
import io.trino.plugin.iceberg.catalog.snowflake.SnowflakeIcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer;
import io.trino.plugin.iceberg.catalog.snowflake.TrinoSnowflakeCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.VarcharType;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.iceberg.catalog.snowflake.TestIcebergSnowflakeCatalogConnectorSmokeTest.S3_ACCESS_KEY;
import static io.trino.plugin.iceberg.catalog.snowflake.TestIcebergSnowflakeCatalogConnectorSmokeTest.S3_REGION;
import static io.trino.plugin.iceberg.catalog.snowflake.TestIcebergSnowflakeCatalogConnectorSmokeTest.S3_SECRET_KEY;
import static io.trino.plugin.iceberg.catalog.snowflake.TestIcebergSnowflakeCatalogConnectorSmokeTest.SNOWFLAKE_S3_EXTERNAL_VOLUME;
import static io.trino.plugin.iceberg.catalog.snowflake.TestIcebergSnowflakeCatalogConnectorSmokeTest.SNOWFLAKE_TEST_SCHEMA;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_JDBC_URI;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_PASSWORD;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_ROLE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_TEST_DATABASE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_USER;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.TableType.ICEBERG;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.snowflake.TrinoIcebergSnowflakeCatalogFactory.getSnowflakeDriverProperties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoSnowflakeCatalog
        extends BaseTrinoCatalogTest
{
    public static final IcebergSnowflakeCatalogConfig CATALOG_CONFIG =
            new IcebergSnowflakeCatalogConfig()
                    .setDatabase(SNOWFLAKE_TEST_DATABASE)
                    .setUri(URI.create(SNOWFLAKE_JDBC_URI))
                    .setRole(SNOWFLAKE_ROLE)
                    .setUser(SNOWFLAKE_USER)
                    .setPassword(SNOWFLAKE_PASSWORD);

    @BeforeAll
    public static void setupServer()
    {
        testTableSetup();
    }

    private static void testTableSetup()
    {
        TestingSnowflakeServer server = new TestingSnowflakeServer();
        try {
            server.execute(SNOWFLAKE_TEST_SCHEMA, "CREATE SCHEMA IF NOT EXISTS %s".formatted(SNOWFLAKE_TEST_SCHEMA));
            if (!server.checkIfTableExists(ICEBERG, SNOWFLAKE_TEST_SCHEMA, TpchTable.NATION.getTableName())) {
                executeOnSnowflake(server, """
                    CREATE OR REPLACE ICEBERG TABLE %s (
                    	NATIONKEY NUMBER(38,0),
                    	NAME STRING,
                    	REGIONKEY NUMBER(38,0),
                    	COMMENT STRING
                    )
                     EXTERNAL_VOLUME = '%s'
                     CATALOG = 'SNOWFLAKE'
                     BASE_LOCATION = '%s/'""".formatted(TpchTable.NATION.getTableName(), SNOWFLAKE_S3_EXTERNAL_VOLUME, TpchTable.NATION.getTableName()));

                executeOnSnowflake(server, "INSERT INTO %s(NATIONKEY, NAME, REGIONKEY, COMMENT) SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.%s"
                        .formatted(TpchTable.NATION.getTableName(), TpchTable.NATION.getTableName()));
            }
            if (!server.checkIfTableExists(ICEBERG, SNOWFLAKE_TEST_SCHEMA, TpchTable.REGION.getTableName())) {
                executeOnSnowflake(server, """
                    CREATE OR REPLACE ICEBERG TABLE %s (
                    	REGIONKEY NUMBER(38,0),
                    	NAME STRING,
                    	COMMENT STRING
                    )
                     EXTERNAL_VOLUME = '%s'
                     CATALOG = 'SNOWFLAKE'
                     BASE_LOCATION = '%s/'""".formatted(TpchTable.REGION.getTableName(), SNOWFLAKE_S3_EXTERNAL_VOLUME, TpchTable.REGION.getTableName()));

                executeOnSnowflake(server, "INSERT INTO %s(REGIONKEY, NAME, COMMENT) SELECT R_REGIONKEY, R_NAME, R_COMMENT FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.%s"
                        .formatted(TpchTable.REGION.getTableName(), TpchTable.REGION.getTableName()));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeOnSnowflake(TestingSnowflakeServer server, String sql)
            throws SQLException
    {
        server.execute(SNOWFLAKE_TEST_SCHEMA, sql);
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
    {
        Map<String, String> properties = getSnowflakeDriverProperties(
                CATALOG_CONFIG.getUri(),
                CATALOG_CONFIG.getUser(),
                CATALOG_CONFIG.getPassword(),
                CATALOG_CONFIG.getRole());
        JdbcClientPool connectionPool = new JdbcClientPool(SNOWFLAKE_JDBC_URI, properties);
        SnowflakeClient snowflakeClient = new JdbcSnowflakeClient(connectionPool);

        S3FileSystemFactory s3FileSystemFactory =
                new S3FileSystemFactory(
                        OpenTelemetry.noop(),
                        new S3FileSystemConfig()
                                .setAwsAccessKey(S3_ACCESS_KEY)
                                .setAwsSecretKey(S3_SECRET_KEY)
                                .setRegion(S3_REGION)
                                .setStreamingPartSize(DataSize.valueOf("5.5MB")), new S3FileSystemStats());

        CatalogName catalogName = new CatalogName("snowflake_test_catalog");
        TrinoIcebergSnowflakeCatalogFileIOFactory catalogFileIOFactory = new TrinoIcebergSnowflakeCatalogFileIOFactory(s3FileSystemFactory, ConnectorIdentity.ofUser("trino"));
        SnowflakeCatalog snowflakeCatalog = new SnowflakeCatalog();
        snowflakeCatalog.initialize(catalogName.toString(), snowflakeClient, catalogFileIOFactory, properties);

        IcebergTableOperationsProvider tableOperationsProvider = new SnowflakeIcebergTableOperationsProvider(CATALOG_CONFIG, s3FileSystemFactory);

        return new TrinoSnowflakeCatalog(
                snowflakeCatalog,
                catalogName,
                TESTING_TYPE_MANAGER,
                s3FileSystemFactory,
                tableOperationsProvider,
                SNOWFLAKE_TEST_DATABASE);
    }

    @Test
    @Override
    public void testCreateNamespaceWithLocation()
    {
        assertThatThrownBy(super::testCreateNamespaceWithLocation)
                .hasMessageContaining("Iceberg Snowflake catalog schemas do not support modifications");
    }

    // Overridden to test against an existing namespace. Additionally, test logic is also changed.
    @Test
    @Override
    public void testNonLowercaseNamespace()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = SNOWFLAKE_TEST_SCHEMA.toUpperCase(ENGLISH);
        // Trino schema names are always lowercase (until https://github.com/trinodb/trino/issues/17)
        String schema = namespace.toLowerCase(ENGLISH);

        // Currently this is actually stored in lowercase by all Catalogs
        assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                .isTrue();
        assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                .isTrue();
        assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                // Catalog listNamespaces may be used as a default implementation for ConnectorMetadata.schemaExists
                .doesNotContain(schema)
                .contains(namespace);

        // Test with IcebergMetadata, should the ConnectorMetadata implementation behavior depend on that class
        ConnectorMetadata icebergMetadata = new IcebergMetadata(
                PLANNER_CONTEXT.getTypeManager(),
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                jsonCodec(CommitTaskData.class),
                catalog,
                (connectorIdentity, fileIOProperties) -> {
                    throw new UnsupportedOperationException();
                },
                new TableStatisticsWriter(new NodeVersion("test-version")));
        assertThat(icebergMetadata.schemaExists(SESSION, namespace)).as("icebergMetadata.schemaExists(namespace)")
                .isTrue();
        assertThat(icebergMetadata.schemaExists(SESSION, schema)).as("icebergMetadata.schemaExists(schema)")
                .isTrue();
        assertThat(icebergMetadata.listSchemaNames(SESSION)).as("icebergMetadata.listSchemaNames")
                .doesNotContain(schema)
                .contains(namespace);
    }

    // Overridden to assert correct method calls as the tested feature is not supported
    @Test
    @Override
    public void testCreateTable()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(SNOWFLAKE_TEST_SCHEMA, table);
        Map<String, String> tableProperties = Map.of("test_key", "test_value");
        String tableLocation = "some/location";
        assertThatThrownBy(
                () -> catalog.newCreateTableTransaction(
                        SESSION,
                        schemaTableName,
                        new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get())),
                        PartitionSpec.unpartitioned(),
                        SortOrder.unsorted(),
                        tableLocation,
                        tableProperties)
                        .commitTransaction())
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    // Overridden to assert correct method calls as the tested feature is not supported
    @Test
    @Override
    public void testCreateWithSortTable()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String namespace = "test_create_sort_table_" + randomNameSuffix();
        String table = "tableName";
        SchemaTableName schemaTableName = new SchemaTableName(namespace, table);
        Schema tableSchema = new Schema(Types.NestedField.of(1, true, "col1", Types.LongType.get()),
                Types.NestedField.of(2, true, "col2", Types.StringType.get()),
                Types.NestedField.of(3, true, "col3", Types.TimestampType.withZone()),
                Types.NestedField.of(4, true, "col4", Types.StringType.get()));

        SortOrder sortOrder = SortOrder.builderFor(tableSchema)
                .asc("col1")
                .desc("col2", NullOrder.NULLS_FIRST)
                .desc("col3")
                .desc(Expressions.year("col3"), NullOrder.NULLS_LAST)
                .desc(Expressions.month("col3"), NullOrder.NULLS_FIRST)
                .asc(Expressions.day("col3"), NullOrder.NULLS_FIRST)
                .asc(Expressions.hour("col3"), NullOrder.NULLS_FIRST)
                .desc(Expressions.bucket("col2", 10), NullOrder.NULLS_FIRST)
                .desc(Expressions.truncate("col4", 5), NullOrder.NULLS_FIRST).build();
        String tableLocation = "some/location";

        assertThatThrownBy(
                () -> catalog.newCreateTableTransaction(
                            SESSION,
                            schemaTableName,
                            tableSchema,
                            PartitionSpec.unpartitioned(),
                            sortOrder,
                            tableLocation,
                            ImmutableMap.of())
                    .commitTransaction())
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    // Overridden to assert correct method calls as the tested feature is not supported
    @Test
    @Override
    public void testRenameTable()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        assertThatThrownBy(
                () -> {
                    SchemaTableName sourceSchemaTableName = new SchemaTableName(SNOWFLAKE_TEST_SCHEMA, TpchTable.REGION.getTableName());
                    // Rename within the same schema
                    SchemaTableName targetSchemaTableName = new SchemaTableName(sourceSchemaTableName.getSchemaName(), "newTableName");
                    catalog.renameTable(SESSION, sourceSchemaTableName, targetSchemaTableName);
                })
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    // Overridden to assert correct method calls as the tested feature is not supported
    @Test
    @Override
    public void testUseUniqueTableLocations()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        String table = "non_existing_table";
        assertThatThrownBy(() -> catalog.defaultTableLocation(SESSION, SchemaTableName.schemaTableName(SNOWFLAKE_TEST_SCHEMA, table)))
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    // Overridden to assert correct method calls as the tested feature is not supported
    @Test
    @Override
    public void testView()
    {
        ConnectorViewDefinition viewDefinition = new ConnectorViewDefinition(
                "SELECT name FROM local.tiny.%s".formatted(TpchTable.NATION.getTableName()),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(
                        new ConnectorViewDefinition.ViewColumn("name", VarcharType.createVarcharType(25).getTypeId(), Optional.empty())),
                Optional.empty(),
                Optional.of(SESSION.getUser()),
                false,
                ImmutableList.of());
        TrinoCatalog catalog = createTrinoCatalog(false);
        assertThatThrownBy(() -> catalog.createView(SESSION, SchemaTableName.schemaTableName(SNOWFLAKE_TEST_SCHEMA, TpchTable.NATION.getTableName()), viewDefinition, true))
                .hasMessageContaining("Views are not supported for the Snowflake Iceberg catalog");
    }

    @Test
    @Override
    public void testListTables()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        List<TableInfo> tables = catalog.listTables(SESSION, Optional.of(SNOWFLAKE_TEST_SCHEMA));
        assertThat(tables.stream().map(table -> table.tableName().getTableName()).toList()).contains(TpchTable.NATION.getTableName(), TpchTable.REGION.getTableName());

        tables = catalog.listTables(SESSION, Optional.of(SNOWFLAKE_TEST_SCHEMA));
        assertThat(tables.stream().map(table -> table.tableName().getTableName()).toList()).contains(TpchTable.NATION.getTableName(), TpchTable.REGION.getTableName());
    }

    @Test
    public void testLoadTable()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Table table = catalog.loadTable(SESSION, SchemaTableName.schemaTableName(SNOWFLAKE_TEST_SCHEMA, TpchTable.REGION.getTableName()));
        assertThat(table.schema().columns().stream().map(Types.NestedField::name).toList())
                .containsExactlyInAnyOrder("REGIONKEY", "NAME", "COMMENT");
    }

    @Test
    public void testLoadNamespaceMetadata()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        Map<String, Object> metadata = catalog.loadNamespaceMetadata(SESSION, SNOWFLAKE_TEST_SCHEMA);
        assertThat(metadata.isEmpty()).isTrue();
    }

    @Test
    public void updateTableComment()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        assertThatThrownBy(
                () -> catalog.updateTableComment(SESSION, SchemaTableName.schemaTableName(SNOWFLAKE_TEST_SCHEMA, TpchTable.REGION.getTableName()), Optional.empty()))
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void defaultTableLocation()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        assertThatThrownBy(() -> catalog.defaultTableLocation(SESSION, SchemaTableName.schemaTableName(SNOWFLAKE_TEST_SCHEMA, TpchTable.REGION.getTableName())))
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void updateColumnComment()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        assertThatThrownBy(
                () -> catalog.updateColumnComment(
                        SESSION,
                        SchemaTableName.schemaTableName(SNOWFLAKE_TEST_SCHEMA, TpchTable.NATION.getTableName()),
                        ColumnIdentity.primitiveColumnIdentity(2, "REGIONKEY"),
                        Optional.of("Column comment created from test code")))
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void listNamespaces()
    {
        TrinoCatalog catalog = createTrinoCatalog(false);
        assertThat(catalog.listNamespaces(SESSION).stream().map(String::toLowerCase).toList()).contains(SNOWFLAKE_TEST_SCHEMA.toLowerCase(ENGLISH));
    }
}
