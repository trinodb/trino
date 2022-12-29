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
import static io.trino.testing.DistributedQueryRunner.builder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public class InfluxQueryRunner
{
    private static final String TPCH_SCHEMA = "tpch";
    private static final Logger LOG = Logger.get(InfluxQueryRunner.class);

    private InfluxQueryRunner() {}

    public static DistributedQueryRunner createInfluxQueryRunner(TestingInfluxServer server, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createInfluxQueryRunner(server, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createInfluxQueryRunner(TestingInfluxServer server, Map<String, String> extraProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createInfluxQueryRunner(server, extraProperties, ImmutableMap.of(), tables);
    }

    public static DistributedQueryRunner createInfluxQueryRunner(TestingInfluxServer server, Map<String, String> extraProperties, Map<String, String> connectorProperties, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = builder(createInfluxSession(TPCH_SCHEMA))
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
        connectorProperties.putIfAbsent("influx.endpoint", server.getEndpoint());
        connectorProperties.putIfAbsent("influx.username", USERNAME);
        connectorProperties.putIfAbsent("influx.password", PASSWORD);
        connectorProperties.putIfAbsent("influx.read-timeout", "5m");
        queryRunner.installPlugin(new InfluxPlugin());
        queryRunner.createCatalog("influxdb", "influxdb", connectorProperties);

        TestingTrinoClient trinoClient = queryRunner.getClient();
        LOG.info("Loading data...");
        InfluxSession session = new InfluxSession(server.getEndpoint());
        new InfluxDataTool(session).setUpDatabase(ImmutableList.of(TPCH_SCHEMA));
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            loadTpchTopic(session, trinoClient, table);
        }
        LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return queryRunner;
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
        DistributedQueryRunner queryRunner = createInfluxQueryRunner(
                new TestingInfluxServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());
        Logger log = Logger.get(InfluxQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
