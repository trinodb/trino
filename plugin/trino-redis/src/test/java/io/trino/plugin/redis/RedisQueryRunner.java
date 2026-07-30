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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.redis.util.CodecSupplier;
import io.trino.plugin.redis.util.RedisCluster;
import io.trino.plugin.redis.util.RedisServer;
import io.trino.plugin.redis.util.RedisTestUtils;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.trino.plugin.redis.util.RedisTestUtils.loadTpchTableDescription;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class RedisQueryRunner
{
    private RedisQueryRunner() {}

    private static final Logger log = Logger.get(RedisQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    public static Builder builder(RedisServer redisServer)
    {
        return new Builder(redisServer);
    }

    public static Builder builder(RedisCluster redisCluster)
    {
        return new Builder(redisCluster);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private RedisServer redisServer;
        private RedisCluster redisCluster;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private String dataFormat;
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private boolean clusterMode;

        private Builder(RedisServer redisServer)
        {
            super(testSessionBuilder()
                    .setCatalog("redis")
                    .setSchema(TPCH_SCHEMA)
                    .build());
            this.redisServer = requireNonNull(redisServer, "redisServer is null");
        }

        private Builder(RedisCluster redisCluster)
        {
            super(testSessionBuilder()
                    .setCatalog("redis")
                    .setSchema(TPCH_SCHEMA)
                    .build());
            this.redisCluster = requireNonNull(redisCluster, "redisCluster is null");
        }

        @CanIgnoreReturnValue
        public Builder setDataFormat(String dataFormat)
        {
            this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(List<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(initialTables);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setClusterMode(boolean clusterMode)
        {
            this.clusterMode = clusterMode;
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                Map<SchemaTableName, RedisTableDescription> tableDescriptions = createTpchTableDescriptions(queryRunner.getPlannerContext().getTypeManager(), initialTables, dataFormat);

                if (redisCluster != null) {
                    installRedisPlugin(redisCluster, queryRunner, tableDescriptions, connectorProperties);
                }
                else {
                    installRedisPlugin(redisServer, queryRunner, tableDescriptions, connectorProperties);
                }

                TestingTrinoClient trinoClient = queryRunner.getClient();

                log.info("Loading data...");
                long startTime = System.nanoTime();
                for (TpchTable<?> table : initialTables) {
                    if (redisCluster != null) {
                        loadTpchTable(redisCluster, trinoClient, table, dataFormat, clusterMode);
                    }
                    else {
                        loadTpchTable(redisServer, trinoClient, table, dataFormat, clusterMode);
                    }
                }
                log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
                if (redisServer != null) {
                    redisServer.closeClient();
                }
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static void loadTpchTable(RedisServer redisServer, TestingTrinoClient trinoClient, TpchTable<?> table, String dataFormat, boolean clusterMode)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        RedisTestUtils.loadTpchTable(
                redisServer,
                trinoClient,
                redisTableName(table),
                new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)),
                dataFormat,
                clusterMode);
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static void loadTpchTable(RedisCluster redisCluster, TestingTrinoClient trinoClient, TpchTable<?> table, String dataFormat, boolean clusterMode)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        RedisTestUtils.loadTpchTable(
                redisCluster,
                trinoClient,
                redisTableName(table),
                new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)),
                dataFormat,
                clusterMode);
        log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String redisTableName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + ":" + table.getTableName().toLowerCase(ENGLISH);
    }

    private static Map<SchemaTableName, RedisTableDescription> createTpchTableDescriptions(TypeManager typeManager, Iterable<TpchTable<?>> tables, String dataFormat)
            throws Exception
    {
        JsonCodec<RedisTableDescription> tableDescriptionJsonCodec = new CodecSupplier<>(RedisTableDescription.class, typeManager).get();

        ImmutableMap.Builder<SchemaTableName, RedisTableDescription> tableDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);

            tableDescriptions.put(loadTpchTableDescription(tableDescriptionJsonCodec, tpchTable, dataFormat));
        }
        return tableDescriptions.buildOrThrow();
    }

    static void main()
            throws Exception
    {
        QueryRunner queryRunner = RedisQueryRunner.builder(new RedisServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setDataFormat("string")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(RedisQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
