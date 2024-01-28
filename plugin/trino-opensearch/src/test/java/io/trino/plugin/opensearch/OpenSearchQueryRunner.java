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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.opensearch.OpenSearchServer.OPENSEARCH_IMAGE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class OpenSearchQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.opensearch.client.RestClient", Level.OFF);
    }

    private OpenSearchQueryRunner() {}

    private static final Logger LOG = Logger.get(OpenSearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    public static QueryRunner createOpenSearchQueryRunner(
            HostAndPort address,
            Iterable<TpchTable<?>> tables,
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            int nodeCount)
            throws Exception
    {
        return createOpenSearchQueryRunner(address, tables, extraProperties, extraConnectorProperties, nodeCount, "opensearch");
    }

    public static QueryRunner createOpenSearchQueryRunner(
            HostAndPort address,
            Iterable<TpchTable<?>> tables,
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            int nodeCount,
            String catalogName)
            throws Exception
    {
        RestHighLevelClient client = null;
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                            .setCatalog(catalogName)
                            .setSchema(TPCH_SCHEMA)
                            .build())
                    .setExtraProperties(extraProperties)
                    .setNodeCount(nodeCount)
                    .build();

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            OpenSearchConnectorFactory testFactory = new OpenSearchConnectorFactory();

            installOpenSearchPlugin(address, queryRunner, catalogName, testFactory, extraConnectorProperties);

            TestingTrinoClient trinoClient = queryRunner.getClient();

            LOG.info("Loading data...");

            client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address.toString())));
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(client, trinoClient, table);
            }
            LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner, client);
            throw e;
        }
    }

    private static void installOpenSearchPlugin(
            HostAndPort address,
            QueryRunner queryRunner,
            String catalogName,
            OpenSearchConnectorFactory factory,
            Map<String, String> extraConnectorProperties)
    {
        queryRunner.installPlugin(new OpenSearchPlugin(factory));
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("opensearch.host", address.getHost())
                .put("opensearch.port", Integer.toString(address.getPort()))
                // Node discovery relies on the publish_address exposed via the OpenSearch API
                // This doesn't work well within a docker environment that maps OpenSearch port to a random public port
                .put("opensearch.ignore-publish-address", "true")
                .put("opensearch.default-schema-name", TPCH_SCHEMA)
                .put("opensearch.scroll-size", "1000")
                .put("opensearch.scroll-timeout", "1m")
                .put("opensearch.request-timeout", "2m")
                .putAll(extraConnectorProperties)
                .buildOrThrow();

        queryRunner.createCatalog(catalogName, "opensearch", config);
    }

    private static void loadTpchTopic(RestHighLevelClient client, TestingTrinoClient trinoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        OpenSearchLoader loader = new OpenSearchLoader(client, table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession());
        loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = createOpenSearchQueryRunner(
                new OpenSearchServer(OPENSEARCH_IMAGE, false, ImmutableMap.of()).getAddress(),
                TpchTable.getTables(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                3);

        Logger log = Logger.get(OpenSearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
