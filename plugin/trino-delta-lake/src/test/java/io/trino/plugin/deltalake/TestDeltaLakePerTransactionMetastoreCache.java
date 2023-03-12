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
import com.google.common.reflect.Reflection;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.Session;
import io.trino.plugin.base.security.UserNameProvider;
import io.trino.plugin.hive.ForHiveMetastore;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.DefaultThriftMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.StaticMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.StaticTokenAwareMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveWriteStatisticsExecutor;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreAuthenticationModule;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreConfig;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreFactory;
import io.trino.plugin.hive.metastore.thrift.TokenAwareMetastoreClientFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchEntity;
import io.trino.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.inject.Singleton;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.security.UserNameProvider.SIMPLE_USER_NAME_PROVIDER;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

@Test(singleThreaded = true) // tests use shared invocation counter map
public class TestDeltaLakePerTransactionMetastoreCache
{
    private static final String BUCKET_NAME = "delta-lake-per-transaction-metastore-cache";
    private HiveMinioDataLake hiveMinioDataLake;
    private ExecutorService executorService;

    private final Map<String, Long> hiveMetastoreInvocationCounts = new ConcurrentHashMap<>();

    private void resetHiveMetastoreInvocationCounts()
    {
        hiveMetastoreInvocationCounts.clear();
    }

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
        executorService = newCachedThreadPool(threadsNamed("hive-thrift-statistics-write-%s"));

        queryRunner.installPlugin(new TestingDeltaLakePlugin(
                Optional.empty(),
                new AbstractConfigurationAwareModule()
                {
                    @Override
                    protected void setup(Binder binder)
                    {
                        newOptionalBinder(binder, ThriftMetastoreClientFactory.class).setDefault().to(DefaultThriftMetastoreClientFactory.class).in(Scopes.SINGLETON);
                        binder.bind(TokenAwareMetastoreClientFactory.class).to(StaticTokenAwareMetastoreClientFactory.class).in(Scopes.SINGLETON);
                        configBinder(binder).bindConfig(StaticMetastoreConfig.class);
                        configBinder(binder).bindConfig(ThriftMetastoreConfig.class);
                        binder.bind(ThriftMetastoreFactory.class).to(ThriftHiveMetastoreFactory.class).in(Scopes.SINGLETON);
                        newExporter(binder).export(ThriftMetastoreFactory.class)
                                .as(generator -> generator.generatedNameOf(ThriftHiveMetastore.class));
                        newOptionalBinder(binder, Key.get(UserNameProvider.class, ForHiveMetastore.class))
                                .setDefault()
                                .toInstance(SIMPLE_USER_NAME_PROVIDER);
                        install(new ThriftMetastoreAuthenticationModule());
                        binder.bind(BridgingHiveMetastoreFactory.class).in(Scopes.SINGLETON);
                        binder.bind(Key.get(boolean.class, AllowDeltaLakeManagedTableRename.class)).toInstance(false);
                    }

                    @Provides
                    @Singleton
                    @RawHiveMetastoreFactory
                    public HiveMetastoreFactory getCountingHiveMetastoreFactory(BridgingHiveMetastoreFactory bridgingHiveMetastoreFactory)
                    {
                        return new HiveMetastoreFactory()
                        {
                            @Override
                            public boolean isImpersonationEnabled()
                            {
                                return false;
                            }

                            @Override
                            public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
                            {
                                HiveMetastore bridgingHiveMetastore = bridgingHiveMetastoreFactory.createMetastore(identity);
                                // bind HiveMetastore which counts method executions
                                return Reflection.newProxy(HiveMetastore.class, (proxy, method, args) -> {
                                    String methodName = method.getName();
                                    long count = hiveMetastoreInvocationCounts.getOrDefault(methodName, 0L);
                                    hiveMetastoreInvocationCounts.put(methodName, count + 1);
                                    return method.invoke(bridgingHiveMetastore, args);
                                });
                            }
                        };
                    }

                    @Provides
                    @Singleton
                    @ThriftHiveWriteStatisticsExecutor
                    public ExecutorService createWriteStatisticsExecutor()
                    {
                        return executorService;
                    }
                }));

        ImmutableMap.Builder<String, String> deltaLakeProperties = ImmutableMap.builder();
        deltaLakeProperties.put("hive.metastore.uri", "thrift://" + hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint());
        deltaLakeProperties.put("hive.s3.aws-access-key", MINIO_ACCESS_KEY);
        deltaLakeProperties.put("hive.s3.aws-secret-key", MINIO_SECRET_KEY);
        deltaLakeProperties.put("hive.s3.endpoint", hiveMinioDataLake.getMinio().getMinioAddress());
        deltaLakeProperties.put("hive.s3.path-style-access", "true");
        deltaLakeProperties.put("hive.metastore", "test"); // use test value so we do not get clash with default bindings)
        deltaLakeProperties.put("hive.metastore-timeout", "1m"); // read timed out sometimes happens with the default timeout
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

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (hiveMinioDataLake != null) {
            hiveMinioDataLake.close();
            hiveMinioDataLake = null;
        }

        if (executorService != null) {
            executorService.shutdown();
        }
    }

    @Test
    public void testPerTransactionHiveMetastoreCachingEnabled()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(true)) {
            resetHiveMetastoreInvocationCounts();
            queryRunner.execute("SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey");
            // Verify cache works; we expect only two calls to `getTable` because we have two tables in a query.
            assertThat(hiveMetastoreInvocationCounts.get("getTable")).isEqualTo(2);
        }
    }

    @Test
    public void testPerTransactionHiveMetastoreCachingDisabled()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(false)) {
            resetHiveMetastoreInvocationCounts();
            queryRunner.execute("SELECT * FROM nation JOIN region ON nation.regionkey = region.regionkey");
            // Sanity check that getTable call is done more than twice if per-transaction cache is disabled.
            // This is to be sure that `testPerTransactionHiveMetastoreCachingEnabled` passes because of per-transation
            // caching and not because of caching done by some other layer.
            assertThat(hiveMetastoreInvocationCounts.get("getTable")).isGreaterThan(2);
        }
    }
}
