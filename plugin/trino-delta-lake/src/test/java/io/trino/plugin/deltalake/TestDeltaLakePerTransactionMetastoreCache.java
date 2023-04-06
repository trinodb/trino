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
import com.google.inject.Binder;
import com.google.inject.Key;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastore;
import io.trino.plugin.hive.metastore.CountingAccessHiveMetastoreUtil;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.CountingAccessHiveMetastore.Methods.GET_TABLE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;

@Test(singleThreaded = true) // tests use shared invocation counter map
public class TestDeltaLakePerTransactionMetastoreCache
{
    private static final String BUCKET_NAME = "delta-lake-per-transaction-metastore-cache";
    private HiveMinioDataLake hiveMinioDataLake;
    private CountingAccessHiveMetastore metastore;

    private DistributedQueryRunner createQueryRunner(boolean enablePerTransactionHiveMetastoreCaching)
            throws Exception
    {
        boolean createdDeltaLake = false;
        if (hiveMinioDataLake == null) {
            // share environment between testcases to speed things up
            hiveMinioDataLake = new HiveMinioDataLake(BUCKET_NAME);
            hiveMinioDataLake.start();
            createdDeltaLake = true;
        }
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema("default")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        metastore = new CountingAccessHiveMetastore(new BridgingHiveMetastore(testingThriftHiveMetastoreBuilder()
                .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        .setMetastoreTimeout(new Duration(1, MINUTES))) // read timed out sometimes happens with the default timeout
                .build()));

        queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.empty(), Optional.empty(), new CountingAccessMetastoreModule(metastore)));

        ImmutableMap.Builder<String, String> deltaLakeProperties = ImmutableMap.builder();
        deltaLakeProperties.put("hive.s3.aws-access-key", MINIO_ACCESS_KEY);
        deltaLakeProperties.put("hive.s3.aws-secret-key", MINIO_SECRET_KEY);
        deltaLakeProperties.put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress());
        deltaLakeProperties.put("hive.s3.path-style-access", "true");
        deltaLakeProperties.put("hive.metastore", "test"); // use test value so we do not get clash with default bindings)
        deltaLakeProperties.put("delta.register-table-procedure.enabled", "true");
        if (!enablePerTransactionHiveMetastoreCaching) {
            // almost disable the cache; 0 is not allowed as config property value
            deltaLakeProperties.put("delta.per-transaction-metastore-cache-maximum-size", "1");
        }

        queryRunner.createCatalog(DELTA_CATALOG, "delta_lake", deltaLakeProperties.buildOrThrow());

        if (createdDeltaLake) {
            List<TpchTable<? extends TpchEntity>> tpchTables = List.of(TpchTable.NATION, TpchTable.REGION);
            tpchTables.forEach(table -> {
                String tableName = table.getTableName();
                hiveMinioDataLake.copyResources("io/trino/plugin/deltalake/testing/resources/databricks/" + tableName, tableName);
                queryRunner.execute(format("CALL %1$s.system.register_table('%2$s', '%3$s', 's3://%4$s/%3$s')",
                        DELTA_CATALOG,
                        "default",
                        tableName,
                        BUCKET_NAME));
            });
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

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (hiveMinioDataLake != null) {
            hiveMinioDataLake.close();
            hiveMinioDataLake = null;
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
            // Sanity check that getTable call is done more than twice if per-transaction cache is disabled.
            // This is to be sure that `testPerTransactionHiveMetastoreCachingEnabled` passes because of per-transaction
            // caching and not because of caching done by some other layer.
            assertMetastoreInvocations(queryRunner, "SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey",
                    ImmutableMultiset.builder()
                            .addCopies(GET_TABLE, 12)
                            .build());
        }
    }

    private void assertMetastoreInvocations(QueryRunner queryRunner, @Language("SQL") String query, Multiset<?> expectedInvocations)
    {
        CountingAccessHiveMetastoreUtil.assertMetastoreInvocations(metastore, queryRunner, queryRunner.getDefaultSession(), query, expectedInvocations);
    }
}
