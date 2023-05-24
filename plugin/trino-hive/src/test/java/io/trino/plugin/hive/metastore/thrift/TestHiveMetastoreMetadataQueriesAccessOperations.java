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
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DataProviders;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.connector.informationschema.InformationSchemaMetadata.MAX_PREFIXES_COUNT;
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
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestHiveMetastoreMetadataQueriesAccessOperations
        extends AbstractTestQueryFramework
{
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
                .build();

        mockMetastore = new MockHiveMetastore();
        metastore = new CountingAccessHiveMetastore(mockMetastore);
        queryRunner.installPlugin(new TestingHivePlugin(metastore));
        queryRunner.createCatalog("hive", "hive", ImmutableMap.of());
        return queryRunner;
    }

    @BeforeMethod
    public void resetMetastoreSetup()
    {
        mockMetastore.setAllTablesViewsImplemented(false);
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectTablesWithoutPredicate(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.tables",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectTablesWithLikeOverSchema(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_schema LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_schem LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectTablesWithFilterByTableName(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name = 'test_table_0'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name = 'test_table_0'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test\\_table\\_0' ESCAPE '\\'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test_table_0' ESCAPE '\\'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectTablesWithLikeOverTableName(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.tables WHERE table_name LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_name LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectViewsWithoutPredicate(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.views",
                allTablesViewsImplemented
                        ? ImmutableMultiset.of(GET_ALL_VIEWS)
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectViewsWithLikeOverSchema(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_schema LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_schem LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectViewsWithFilterByTableName(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name = 'test_table_0'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name = 'test_table_0'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectViewsWithLikeOverTableName(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.views WHERE table_name LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.tables WHERE table_type = 'VIEW' AND table_name LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectColumnsWithoutPredicate(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.columns",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectColumnsWithLikeOverSchema(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_schema LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectColumnsFilterByTableName(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        metastore.resetCounters();
        computeActual("SELECT * FROM information_schema.columns WHERE table_name = 'test_table_0'");
        Multiset<Method> invocations = metastore.getMethodInvocations();

        assertThat(invocations.count(GET_TABLE)).as("GET_TABLE invocations")
                // some lengthy explanatory comment why variable count
                // TODO switch to assertMetastoreInvocations when GET_TABLE invocation count becomes deterministic
                // TODO this is horribly lot, why, o why we do those redirect-related calls in ... even if redirects not enabled?????????!!?!1
                .isBetween(TEST_ALL_TABLES_COUNT + 85, TEST_ALL_TABLES_COUNT + 95);
        invocations = HashMultiset.create(invocations);
        invocations.elementSet().remove(GET_TABLE);

        assertThat(invocations).as("invocations except of GET_TABLE")
                .isEqualTo(allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .build()
                        : ImmutableMultiset.builder()
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectColumnsWithLikeOverTableName(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE table_name LIKE 'test%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
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

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectColumnsFilterByColumn(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name = 'name'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name = 'name'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testSelectColumnsWithLikeOverColumn(boolean allTablesViewsImplemented)
    {
        mockMetastore.setAllTablesViewsImplemented(allTablesViewsImplemented);
        assertMetastoreInvocations("SELECT * FROM information_schema.columns WHERE column_name LIKE 'n%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
                        .add(GET_ALL_DATABASES)
                        .addCopies(GET_ALL_TABLES_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_ALL_VIEWS_FROM_DATABASE, TEST_SCHEMAS_COUNT)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build());
        assertMetastoreInvocations("SELECT * FROM system.jdbc.columns WHERE column_name LIKE 'n%'",
                allTablesViewsImplemented
                        ? ImmutableMultiset.builder()
                        .add(GET_ALL_TABLES)
                        .add(GET_ALL_VIEWS)
                        .addCopies(GET_TABLE, TEST_ALL_TABLES_COUNT)
                        .build()
                        : ImmutableMultiset.builder()
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
