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
package io.trino.plugin.deltalake.metastore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.Session;
import io.trino.plugin.deltalake.AllowDeltaLakeManagedTableRename;
import io.trino.plugin.deltalake.TestingDeltaLakePlugin;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_DATABASE;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_TABLE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true) // metastore invocation counters shares mutable state so can't be run from many threads simultaneously
public class TestDeltaLakeMetastoreAccessOperations
        extends AbstractTestQueryFramework
{
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog("delta_lake")
            .setSchema("test_schema")
            .build();

    private CountingAccessHiveMetastore metastore;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake").toFile();
        metastore = new CountingAccessHiveMetastore(createTestingFileHiveMetastore(baseDir));

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.empty(), new CountingAccessMetastoreModule(metastore)));
        ImmutableMap.Builder<String, String> deltaLakeProperties = ImmutableMap.builder();
        deltaLakeProperties.put("hive.metastore", "test"); // use test value so we do not get clash with default bindings)
        queryRunner.createCatalog("delta_lake", "delta_lake", deltaLakeProperties.buildOrThrow());

        queryRunner.execute("CREATE SCHEMA test_schema");
        return queryRunner;
    }

    private static class CountingAccessMetastoreModule
            extends AbstractConfigurationAwareModule
    {
        private final CountingAccessHiveMetastore metastore;

        public CountingAccessMetastoreModule(CountingAccessHiveMetastore metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
        }

        @Override
        protected void setup(Binder binder)
        {
            binder.bind(HiveMetastoreFactory.class).annotatedWith(RawHiveMetastoreFactory.class).toInstance(HiveMetastoreFactory.ofInstance(metastore));
            binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
        }
    }

    @Test
    public void testCreateTable()
    {
        assertMetastoreInvocations("CREATE TABLE test_create (id VARCHAR, age INT)",
                ImmutableMultiset.builder()
                        .add(CREATE_TABLE)
                        .add(GET_DATABASE)
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertMetastoreInvocations("CREATE TABLE test_ctas AS SELECT 1 AS age",
                ImmutableMultiset.builder()
                        .add(GET_DATABASE)
                        .add(CREATE_TABLE)
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelect()
    {
        assertUpdate("CREATE TABLE test_select_from (id VARCHAR, age INT)");

        assertMetastoreInvocations("SELECT * FROM test_select_from",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_from_where AS SELECT 2 as age", 1);

        assertMetastoreInvocations("SELECT * FROM test_select_from_where WHERE age = 2",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testSelectFromView()
    {
        assertUpdate("CREATE TABLE test_select_view_table (id VARCHAR, age INT)");
        assertUpdate("CREATE VIEW test_select_view_view AS SELECT id, age FROM test_select_view_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_view",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_view_where_table AS SELECT 2 as age", 1);
        assertUpdate("CREATE VIEW test_select_view_where_view AS SELECT age FROM test_select_view_where_table");

        assertMetastoreInvocations("SELECT * FROM test_select_view_where_view WHERE age = 2",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelectFromMaterializedView()
    {
        assertUpdate("CREATE TABLE test_select_mview_table (id VARCHAR, age INT)");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_select_mview_view AS SELECT id, age FROM test_select_mview_table",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testSelectFromMaterializedViewWithFilter()
    {
        assertUpdate("CREATE TABLE test_select_mview_where_table AS SELECT 2 as age", 1);
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_select_mview_where_view AS SELECT age FROM test_select_mview_where_table",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testRefreshMaterializedView()
    {
        assertUpdate("CREATE TABLE test_refresh_mview_table (id VARCHAR, age INT)");
        assertQueryFails(
                "CREATE MATERIALIZED VIEW test_refresh_mview_view AS SELECT id, age FROM test_refresh_mview_table",
                "This connector does not support creating materialized views");
    }

    @Test
    public void testJoin()
    {
        assertUpdate("CREATE TABLE test_join_t1 AS SELECT 2 as age, 'id1' AS id", 1);
        assertUpdate("CREATE TABLE test_join_t2 AS SELECT 'name1' as name, 'id1' AS id", 1);

        assertMetastoreInvocations("SELECT name, age FROM test_join_t1 JOIN test_join_t2 ON test_join_t2.id = test_join_t1.id",
                ImmutableMultiset.builder()
                        .addCopies(GET_TABLE, 2)
                        .build());
    }

    @Test
    public void testSelfJoin()
    {
        assertUpdate("CREATE TABLE test_self_join_table AS SELECT 2 as age, 0 parent, 3 AS id", 1);

        assertMetastoreInvocations("SELECT child.age, parent.age FROM test_self_join_table child JOIN test_self_join_table parent ON child.parent = parent.id",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testExplainSelect()
    {
        assertUpdate("CREATE TABLE test_explain AS SELECT 2 as age", 1);

        assertMetastoreInvocations("EXPLAIN SELECT * FROM test_explain",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testShowStatsForTable()
    {
        assertUpdate("CREATE TABLE test_show_stats AS SELECT 2 as age", 1);

        assertMetastoreInvocations("SHOW STATS FOR test_show_stats",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    @Test
    public void testShowStatsForTableWithFilter()
    {
        assertUpdate("CREATE TABLE test_show_stats_with_filter AS SELECT 2 as age", 1);

        assertMetastoreInvocations("SHOW STATS FOR (SELECT * FROM test_show_stats_with_filter where age >= 2)",
                ImmutableMultiset.builder()
                        .add(GET_TABLE)
                        .build());
    }

    private void assertMetastoreInvocations(@Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        metastore.resetCounters();
        getQueryRunner().execute(query);
        Multiset<CountingAccessHiveMetastore.Methods> actualInvocations = metastore.getMethodInvocations();

        if (expectedInvocations.equals(actualInvocations)) {
            return;
        }

        List<String> mismatchReport = Sets.union(expectedInvocations.elementSet(), actualInvocations.elementSet()).stream()
                .filter(key -> expectedInvocations.count(key) != actualInvocations.count(key))
                .flatMap(key -> {
                    int expectedCount = expectedInvocations.count(key);
                    int actualCount = actualInvocations.count(key);
                    if (actualCount < expectedCount) {
                        return Stream.of(format("%s more occurrences of %s", expectedCount - actualCount, key));
                    }
                    if (actualCount > expectedCount) {
                        return Stream.of(format("%s fewer occurrences of %s", actualCount - expectedCount, key));
                    }
                    return Stream.of();
                })
                .collect(toImmutableList());

        fail("Expected: \n\t\t" + join(",\n\t\t", mismatchReport));
    }
}
