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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.influxdb.TestingInfluxServer.PASSWORD;
import static io.trino.plugin.influxdb.TestingInfluxServer.USERNAME;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public class InfluxQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";
    private static final Logger LOG = Logger.get(InfluxQueryRunner.class);

    private InfluxQueryRunner() {}

    // Builder pattern implementation
    public static Builder builder(TestingInfluxServer server)
    {
        return new Builder(server);
    }

    public static class Builder
    {
        private final TestingInfluxServer server;
        private Map<String, String> extraProperties = ImmutableMap.of();
        private Map<String, String> connectorProperties = ImmutableMap.of();
        private Iterable<TpchTable<?>> tables = ImmutableList.of();
        private String schema = TPCH_SCHEMA;

        public Builder(TestingInfluxServer server)
        {
            this.server = server;
        }

        public Builder withExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = (extraProperties == null) ? ImmutableMap.of() : extraProperties;
            return this;
        }

        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = (connectorProperties == null) ? ImmutableMap.of() : connectorProperties;
            return this;
        }

        public Builder withTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = (tables == null) ? ImmutableList.of() : tables;
            return this;
        }

        public Builder withSchema(String schema)
        {
            this.schema = (schema == null) ? TPCH_SCHEMA : schema;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createInfluxSession(schema))
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> mutableConnectorProps = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            mutableConnectorProps.putIfAbsent("influx.endpoint", server.getEndpoint());
            mutableConnectorProps.putIfAbsent("influx.insecure-allowed", "true");
            mutableConnectorProps.putIfAbsent("influx.username", USERNAME);
            mutableConnectorProps.putIfAbsent("influx.password", PASSWORD);
            mutableConnectorProps.putIfAbsent("influx.read-timeout", "5m");
            queryRunner.installPlugin(new InfluxPlugin());
            queryRunner.createCatalog("influxdb", "influxdb", mutableConnectorProps);

            // Load data if tables provided
            if (tables != null) {
                TestingTrinoClient trinoClient = queryRunner.getClient();
                LOG.info("Loading data...");
                InfluxSession session = new InfluxSession(server.getEndpoint());
                new InfluxDataTool(session).setUpDatabase(ImmutableList.of(TPCH_SCHEMA));
                long startTime = System.nanoTime();
                for (TpchTable<?> table : tables) {
                    loadTpchTopic(session, trinoClient, table);
                }
                LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
            }

            return queryRunner;
        }
    }

    public static DistributedQueryRunner createInfluxQueryRunner(TestingInfluxServer server, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return builder(server)
                .withTables(ImmutableList.copyOf(tables))
                .build();
    }

    public static DistributedQueryRunner createInfluxQueryRunner(TestingInfluxServer server, Map<String, String> extraProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return builder(server)
                .withExtraProperties(extraProperties)
                .withTables(tables)
                .build();
    }

    public static DistributedQueryRunner createInfluxQueryRunner(TestingInfluxServer server, Map<String, String> extraProperties, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return builder(server)
                .withExtraProperties(extraProperties)
                .withConnectorProperties(connectorProperties)
                .withTables(tables)
                .build();
    }

    public static Session createInfluxSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("influxdb")
                .setSchema(schema)
                .build();
    }

    private static void loadTpchTopic(InfluxSession session, TestingTrinoClient trinoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        String tableName = table.getTableName().toLowerCase(ENGLISH);
        InfluxLoader loader = new InfluxLoader(session, TPCH_SCHEMA, tableName, trinoClient.getServer(), trinoClient.getDefaultSession());
        loader.execute(format("SELECT * FROM %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = builder(new TestingInfluxServer())
//                .withExtraProperties(ImmutableMap.of("http-server.http.port", "8020"))
                .withTables(TpchTable.getTables())
                .build();
        Logger log = Logger.get(InfluxQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
