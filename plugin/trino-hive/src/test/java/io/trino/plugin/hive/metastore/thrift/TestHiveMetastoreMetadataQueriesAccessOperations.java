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
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_DATABASES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_TABLES;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_TABLES_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_VIEWS;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_ALL_VIEWS_FROM_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
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

    private MockHiveMetastore mockMetastore;
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
                // metadata queries do not use workers
                .setNodeCount(1)
                .addCoordinatorProperty("optimizer.experimental-max-prefetched-information-schema-prefixes", Integer.toString(MAX_PREFIXES_COUNT))
                .build();

        mockMetastore = new MockHiveMetastore();
        metastore = new CountingAccessHiveMetastore(mockMetastore);
        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of());
        return queryRunner;
    }

    private void resetMetastoreSetup()
    {
        mockMetastore.setAllTablesViewsImplemented(false);
    }

    @Test
    public void testSelectSchemasWithoutPredicate()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations("SELECT * FROM information_schema.schemata", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithFilterByInformationSchema()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem = 'information_schema'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectSchemasWithLikeOverSchemaName()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations("SELECT * FROM information_schema.schemata WHERE schema_name LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
        assertMetastoreInvocations("SELECT * FROM system.jdbc.schemas WHERE table_schem LIKE 'test%'", ImmutableMultiset.of(GET_ALL_DATABASES));
    }

    @Test
    public void testSelectTablesWithoutPredicate()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        Multiset<Method> tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_TABLES)
                .add(GET_ALL_VIEWS)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.tables", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables", tables);

        mockMetastore.setAllTablesViewsImplemented(false);
        tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_DATABASES)
                .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.tables", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables", tables);
    }

    @Test
    public void testSelectTablesWithFilterByInformationSchema()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectTablesWithFilterBySchema()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_schem = 'test_schema_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES_FROM_DATABASE)
                        .add(GET_ALL_VIEWS_FROM_DATABASE)
                        .build());
    }

    @Test
    public void testSelectTablesWithLikeOverSchema()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        Multiset<Method> tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_DATABASES)
                .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'", tables);
    }

    @Test
    public void testSelectTablesWithFilterByTableName()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        Multiset<Method> tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_TABLES)
                .add(GET_ALL_VIEWS)
                .build();
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'", tables);

        mockMetastore.setAllTablesViewsImplemented(false);
        tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_DATABASES)
                .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'", tables);
    }

    @Test
    public void testSelectTablesWithLikeOverTableName()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        Multiset<Method> tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_DATABASES)
                .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'", tables);
    }

    @Test
    public void testSelectViewsWithoutPredicate()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations("SELECT * FROM information_schema.views", ImmutableMultiset.of(GET_ALL_VIEWS));
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByInformationSchema()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectViewsWithFilterBySchema()
    {
        resetMetastoreSetup();

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
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectViewsWithFilterByTableName()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectViewsWithLikeOverTableName()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithoutPredicate()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
        ImmutableMultiset<Method> tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_TABLES)
                .add(GET_ALL_VIEWS)
                .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.columns", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns", tables);

        mockMetastore.setAllTablesViewsImplemented(false);
        tables = ImmutableMultiset.<Method>builder()
                .add(GET_ALL_DATABASES)
                .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                .build();
        assertMetastoreInvocations("SELECT * FROM information_schema.columns", tables);
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns", tables);
    }

    @Test
    public void testSelectColumnsFilterByInformationSchema()
    {
        resetMetastoreSetup();

        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema = 'information_schema'", ImmutableMultiset.of());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE table_schem = 'information_schema'", ImmutableMultiset.of());
    }

    @Test
    public void testSelectColumnsFilterBySchema()
    {
        resetMetastoreSetup();

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
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
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

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE table_schema LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
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
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
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
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        // TODO When there are many schemas, there are no "prefixes" and we end up calling ConnectorMetadata without any filter whatsoever.
                        //  If such queries are common enough, we could iterate over schemas and for each schema try getting a table by given name.
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name = 'test_table_0'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsWithLikeOverTableName()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
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
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE table_name LIKE 'test%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test
    public void testSelectColumnsFilterByColumn()
    {
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
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

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE column_name = 'name'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE column_name = 'name'",
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
        resetMetastoreSetup();

        mockMetastore.setAllTablesViewsImplemented(true);
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

        mockMetastore.setAllTablesViewsImplemented(false);
        assertMetastoreInvocations(
                "SELECT * FROM information_schema.columns WHERE column_name LIKE 'n%'",
                ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations(
                "SELECT * FROM system.jdbc.columns WHERE column_name LIKE 'n%'",
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
        resetMetastoreSetup();

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

    private static class MockHiveMetastore
            extends UnimplementedHiveMetastore
    {
        private static final List<String> SCHEMAS = IntStream.range(0, TEST_SCHEMAS_COUNT)
                .mapToObj("test_schema_%d"::formatted)
                .collect(toImmutableList());
        private static final List<String> TABLES_PER_SCHEMA = IntStream.range(0, TEST_TABLES_IN_SCHEMA_COUNT)
                .mapToObj("test_table_%d"::formatted)
                .collect(toImmutableList());
        private static final ImmutableList<SchemaTableName> ALL_TABLES = SCHEMAS.stream()
                .flatMap(schema -> TABLES_PER_SCHEMA.stream()
                        .map(table -> new SchemaTableName(schema, table)))
                .collect(toImmutableList());

        private boolean allTablesViewsImplemented;

        @Override
        public List<String> getAllDatabases()
        {
            return SCHEMAS;
        }

        @Override
        public List<String> getAllTables(String databaseName)
        {
            return TABLES_PER_SCHEMA;
        }

        @Override
        public Optional<List<SchemaTableName>> getAllTables()
        {
            if (allTablesViewsImplemented) {
                return Optional.of(ALL_TABLES);
            }
            return Optional.empty();
        }

        @Override
        public List<String> getAllViews(String databaseName)
        {
            return ImmutableList.of();
        }

        @Override
        public Optional<List<SchemaTableName>> getAllViews()
        {
            if (allTablesViewsImplemented) {
                return Optional.of(ImmutableList.of());
            }
            return Optional.empty();
        }

        @Override
        public Optional<Table> getTable(String databaseName, String tableName)
        {
            return Optional.of(Table.builder()
                    .setDatabaseName(databaseName)
                    .setTableName(tableName)
                    .setDataColumns(ImmutableList.of(
                            new Column("id", HiveType.HIVE_INT, Optional.empty()),
                            new Column("name", HiveType.HIVE_STRING, Optional.empty())))
                    .setOwner(Optional.empty())
                    .setTableType(MANAGED_TABLE.name())
                    .withStorage(storage ->
                            storage.setStorageFormat(fromHiveStorageFormat(ORC))
                                    .setLocation(Optional.empty()))
                    .build());
        }

        public void setAllTablesViewsImplemented(boolean allTablesViewsImplemented)
        {
            this.allTablesViewsImplemented = allTablesViewsImplemented;
        }
    }
}
