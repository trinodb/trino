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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreMethod;
import io.trino.plugin.hive.metastore.Table;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_RELATION_TYPES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_TABLES;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_TABLES_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_VIEWS;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_ALL_VIEWS_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_RELATION_TYPES_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.MetastoreMethod.GET_TABLE;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestHiveMetastoreMetadataQueriesAccessOperations
        extends AbstractTestQueryFramework
{
    private static final int MAX_PREFIXES_COUNT = 20;
    private static final int TEST_SCHEMAS_COUNT = MAX_PREFIXES_COUNT + 1;
    private static final int TEST_TABLES_IN_SCHEMA_COUNT = MAX_PREFIXES_COUNT + 3;
    private static final int TEST_ALL_TABLES_COUNT = TEST_SCHEMAS_COUNT * TEST_TABLES_IN_SCHEMA_COUNT;

    private HiveHadoop hiveHadoop;
    private CountingAccessHiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveHadoop = HiveHadoop.builder().build();
        hiveHadoop.start();

        HiveMetastore thriftMetastore = new BridgingHiveMetastore(testingThriftHiveMetastoreBuilder()
                .metastoreClient(hiveHadoop.getHiveMetastoreEndpoint())
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        .setBatchMetadataFetchEnabled(true)
                        .setDeleteFilesOnDrop(true))
                .hiveConfig(new HiveConfig().setTranslateHiveViews(true))
                .build());
        for (int databaseId = 0; databaseId < TEST_SCHEMAS_COUNT; databaseId++) {
            String databaseName = "test_schema_" + databaseId;
            thriftMetastore.createDatabase(Database.builder()
                    .setDatabaseName(databaseName)
                    .setOwnerName(Optional.empty())
                    .setOwnerType(Optional.empty())
                    .build());

            for (int tableId = 0; tableId < TEST_TABLES_IN_SCHEMA_COUNT; tableId++) {
                Table.Builder table = Table.builder()
                        .setDatabaseName(databaseName)
                        .setTableName("test_table_" + tableId)
                        .setTableType(MANAGED_TABLE.name())
                        .setDataColumns(ImmutableList.of(
                                new Column("id", HiveType.HIVE_INT, Optional.empty(), Map.of()),
                                new Column("name", HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                        .setOwner(Optional.empty());
                table.getStorageBuilder()
                        .setStorageFormat(fromHiveStorageFormat(PARQUET));
                thriftMetastore.createTable(table.build(), NO_PRIVILEGES);
            }
        }

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("hive")
                                .setSchema(Optional.empty())
                                .build())
                // metadata queries do not use workers
                .setNodeCount(1)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();

        metastore = new CountingAccessHiveMetastore(thriftMetastore);

        queryRunner.installPlugin(new TestingHivePlugin(queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data"), metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of());
        return queryRunner;
    }

    @AfterAll
    void afterAll()
    {
        hiveHadoop.stop();
    }

    @Test
    public void testSelectSchemasWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithLikeOverSchemaName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectTablesWithoutPredicate()
    {
        Multiset<MetastoreMethod> tables = ImmutableMultiset.<MetastoreMethod>builder()
                .add(GET_ALL_RELATION_TYPES)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.tables", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables", tables);
    }

    @Test
    public void testSelectTablesWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectTablesWithFilterBySchema()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_RELATION_TYPES_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_RELATION_TYPES_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverSchema()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
    }

    @Test
    public void testSelectTablesWithFilterByTableName()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_RELATION_TYPES)
                        .build());

        Multiset<MetastoreMethod> tables = ImmutableMultiset.<MetastoreMethod>builder()
                .add(GET_ALL_RELATION_TYPES)
                .build();
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'", tables);
    }

    @Test
    public void testSelectTablesWithLikeOverTableName()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
    }

    @Test
    public void testSelectViewsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views", ImmutableMultiset.of(GET_ALL_VIEWS));
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectViewsWithFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'test_schema_0'", ImmutableMultiset.of(GET_ALL_VIEWS_FROM_DATABASE));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_RELATION_TYPES_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverSchema()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByTableName()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverTableName()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_RELATION_TYPES)
                        .build());
    }

    @Test
    public void testSelectColumnsWithoutPredicate()
    {
        ImmutableMultiset<MetastoreMethod> tables = ImmutableMultiset.<MetastoreMethod>builder()
                .add(GET_ALL_TABLES)
                .add(GET_ALL_VIEWS)
                .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.columns", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns", tables);
    }

    @Test
    public void testSelectColumnsFilterByInformationSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectColumnsFilterBySchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .addCopies(GET_TABLE, TEST_TABLES_IN_SCHEMA_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .addCopies(GET_TABLE, TEST_TABLES_IN_SCHEMA_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test\\_schema\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .addCopies(GET_TABLE, TEST_TABLES_IN_SCHEMA_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test_schema_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .addCopies(GET_TABLE, TEST_TABLES_IN_SCHEMA_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverSchema()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableName()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        // TODO When there are many schemas, there are no "prefixes" and we end up calling ConnectorMetadata without any filter whatsoever.
                        //  If such queries are common enough, we could iterate over schemas and for each schema try getting a table by given name.
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT + 1)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT + 1)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT + 1)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByColumn()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverColumn()
    {
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableAndSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'test_schema_0' AND table_name = 'test_table_0'", ImmutableMultiset.of(GET_TABLE));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'test_schema_0' AND table_name = 'test_table_0'", ImmutableMultiset.of(GET_TABLE));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test\\_schema\\_0' ESCAPE '\\' AND table_name LIKE 'test\\_table\\_0' ESCAPE '\\'", ImmutableMultiset.of(GET_TABLE));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test_schema_0' ESCAPE '\\' AND table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_TABLE)
                        .build());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, getQueryRunner(), getQueryRunner().getDefaultSession(), query, expectedInvocations);
    }
}
