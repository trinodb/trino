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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.reflect.ClassPath;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.Session;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Method.GET_TABLE;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.write;
import static java.util.Objects.requireNonNull;

@Test(singleThreaded = true) // tests use shared invocation counter map
public class TestDeltaLakePerTransactionMetastoreCache
{
    private CountingAccessHiveMetastore metastore;

    private DistributedQueryRunner createQueryRunner(boolean enablePerTransactionHiveMetastoreCaching)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            FileHiveMetastore fileMetastore = createTestingFileHiveMetastore(queryRunner.getCoordinator().getBaseDataDir().resolve("file-metastore").toFile());
            metastore = new CountingAccessHiveMetastore(fileMetastore);
            queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.empty(), Optional.empty(), new CountingAccessMetastoreModule(metastore)));

            ImmutableMap.Builder<String, String> deltaLakeProperties = ImmutableMap.builder();
            deltaLakeProperties.put("hive.metastore", "test"); // use test value so we do not get clash with default bindings)
            deltaLakeProperties.put("delta.register-table-procedure.enabled", "true");
            if (!enablePerTransactionHiveMetastoreCaching) {
                // almost disable the cache; 0 is not allowed as config property value
                deltaLakeProperties.put("delta.per-transaction-metastore-cache-maximum-size", "1");
            }

            queryRunner.createCatalog(DELTA_CATALOG, "delta_lake", deltaLakeProperties.buildOrThrow());
            queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());

            for (TpchTable<? extends TpchEntity> table : List.of(TpchTable.NATION, TpchTable.REGION)) {
                String tableName = table.getTableName();
                String resourcePath = "io/trino/plugin/deltalake/testing/resources/databricks/" + tableName + "/";
                Path tableDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("%s-%s".formatted(tableName, randomNameSuffix()));

                for (ClassPath.ResourceInfo resourceInfo : ClassPath.from(getClass().getClassLoader()).getResources()) {
                    if (resourceInfo.getResourceName().startsWith(resourcePath)) {
                        Path targetFile = tableDirectory.resolve(resourceInfo.getResourceName().substring(resourcePath.length()));
                        createDirectories(targetFile.getParent());
                        write(targetFile, resourceInfo.asByteSource().read());
                    }
                }

                queryRunner.execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", tableName, tableDirectory));
            }
        }
        catch (Throwable e) {
            Closables.closeAllSuppress(e, queryRunner);
            throw e;
        }

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
    public void testPerTransactionHiveMetastoreCachingEnabled()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(true)) {
            // Verify cache works; we expect only two calls to `getTable` because we have two tables in a query.
            assertMetastoreInvocations(queryRunner, "SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
    }

    @Test
    public void testPerTransactionHiveMetastoreCachingDisabled()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(false)) {
            assertMetastoreInvocations(queryRunner, "SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 2)
                            .build());
        }
    }

    private void assertMetastoreInvocations(QueryRunner queryRunner, @Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, queryRunner, queryRunner.getDefaultSession(), query, expectedInvocations);
    }
}
