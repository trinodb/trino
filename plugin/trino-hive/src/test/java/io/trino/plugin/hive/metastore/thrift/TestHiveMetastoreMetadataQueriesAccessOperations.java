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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.connector.informationschema.InformationSchemaMetadata.MAX_PREFIXES_COUNT;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_ALL_TABLES_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_ALL_VIEWS_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_TABLE;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestHiveMetastoreMetadataQueriesAccessOperations
        extends AbstractTestQueryFramework
{
    private static final int TEST_SCHEMAS_COUNT = MAX_PREFIXES_COUNT + 1;
    private static final int TEST_TABLES_IN_SCHEMA_COUNT = MAX_PREFIXES_COUNT + 3;
    private static final int TEST_ALL_TABLES_COUNT = TEST_SCHEMAS_COUNT * TEST_TABLES_IN_SCHEMA_COUNT;

    private CountingAccessHiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("hive")
                                .setSchema(Optional.empty())
                                .build())
                .build();

        Path baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive");
        FileHiveMetastore fileHiveMetastore = createTestingFileHiveMetastore(baseDir.toFile());
        metastore = new CountingAccessHiveMetastore(fileHiveMetastore);

        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of());

        for (int schemaNumber = 0; schemaNumber < TEST_SCHEMAS_COUNT; schemaNumber++) {
            String schemaName = "test_schema_%d".formatted(schemaNumber);
            Path schemaPath = baseDir.resolve(schemaName);
            fileHiveMetastore.createDatabase(prepareSchema(schemaName));
            for (int tableNumber = 0; tableNumber < TEST_TABLES_IN_SCHEMA_COUNT; tableNumber++) {
                String tableName = "test_table_%d".formatted(tableNumber);
                Path tablePath = schemaPath.resolve(tableName);
                fileHiveMetastore.createTable(prepareTable(schemaName, tableName, tablePath), NO_PRIVILEGES);
            }
        }
        return queryRunner;
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
        assertMetastoreInvocations("SELECT * FROM information_schema.tables",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
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
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectTablesWithFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectViewsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
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
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverSchema()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverTableName()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithoutPredicate()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
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
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByTableName()
    {
        metastore.resetCounters();
        computeActual("SELECT * FROM information_schema.columns WHERE table_name = 'test_table_0'");
        Multiset<Methods> invocations = metastore.getMethodInvocations();

        assertThat(invocations.count(GET_TABLE)).as("GET_TABLE invocations")
                // some lengthy explanatory comment why variable count
                // TODO switch to assertMetastoreInvocations when GET_TABLE invocation count becomes deterministic
                // TODO this is horribly lot, why, o why we do those redirect-related calls in ... even if redirects not enabled?????????!!?!1
                .isBetween(TEST_ALL_TABLES_COUNT + 85, TEST_ALL_TABLES_COUNT + 95);
        invocations = HashMultiset.create(invocations);
        invocations.elementSet().remove(GET_TABLE);

        assertThat(invocations).as("invocations except of GET_TABLE")
                .isEqualTo(ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());

        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
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
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByColumn()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverColumn()
    {
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
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

    private static Database prepareSchema(String schemaName)
    {
        return Database.builder()
                .setDatabaseName(schemaName)
                .setOwnerName(Optional.empty())
                .setOwnerType(Optional.empty())
                .build();
    }

    private static Table prepareTable(String schemaName, String tableName, Path tablePath)
    {
        return Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setDataColumns(ImmutableList.of(
                        new Column("id", HiveType.HIVE_INT, Optional.empty()),
                        new Column("name", HiveType.HIVE_STRING, Optional.empty())))
                .setOwner(Optional.empty())
                .setTableType(MANAGED_TABLE.name())
                .withStorage(storage ->
                        storage.setStorageFormat(fromHiveStorageFormat(ORC))
                                .setLocation(Optional.of(tablePath.toUri().toString())))
                .build();
    }
}
