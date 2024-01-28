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
package io.trino.plugin.elasticsearch;

import com.google.common.collect.ImmutableMap;
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
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_7_IMAGE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class ElasticsearchQueryRunner
{
    public static final String USER = "elastic_user";
    public static final String PASSWORD = "123456";

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.elasticsearch.client.RestClient", Level.OFF);
    }

    private ElasticsearchQueryRunner() {}

    private static final Logger LOG = Logger.get(ElasticsearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    public static QueryRunner createElasticsearchQueryRunner(
            ElasticsearchServer server,
            Iterable<TpchTable<?>> tables,
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            int nodeCount)
            throws Exception
    {
        return createElasticsearchQueryRunner(server, tables, extraProperties, extraConnectorProperties, nodeCount, "elasticsearch");
    }

    public static QueryRunner createElasticsearchQueryRunner(
            ElasticsearchServer server,
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

            ElasticsearchConnectorFactory testFactory = new ElasticsearchConnectorFactory();

            installElasticsearchPlugin(server, queryRunner, catalogName, testFactory, extraConnectorProperties);

            TestingTrinoClient trinoClient = queryRunner.getClient();

            LOG.info("Loading data...");

            client = server.getClient();

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

    public static SSLContext getSSLContext()
    {
        try {
            return createSSLContext(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new File(getResource("truststore.jks").toURI())),
                    Optional.of("123456"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void installElasticsearchPlugin(
            ElasticsearchServer server,
            QueryRunner queryRunner,
            String catalogName,
            ElasticsearchConnectorFactory factory,
            Map<String, String> extraConnectorProperties)
            throws URISyntaxException
    {
        queryRunner.installPlugin(new ElasticsearchPlugin(factory));
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("elasticsearch.host", server.getAddress().getHost())
                .put("elasticsearch.port", Integer.toString(server.getAddress().getPort()))
                // Node discovery relies on the publish_address exposed via the Elasticseach API
                // This doesn't work well within a docker environment that maps ES's port to a random public port
                .put("elasticsearch.ignore-publish-address", "true")
                .put("elasticsearch.default-schema-name", TPCH_SCHEMA)
                .put("elasticsearch.scroll-size", "1000")
                .put("elasticsearch.scroll-timeout", "1m")
                .put("elasticsearch.request-timeout", "2m")
                .put("elasticsearch.tls.enabled", "true")
                .put("elasticsearch.tls.truststore-path", new File(getResource("truststore.jks").toURI()).getPath())
                .put("elasticsearch.tls.truststore-password", "123456")
                .put("elasticsearch.tls.verify-hostnames", "false")
                .put("elasticsearch.security", "PASSWORD")
                .put("elasticsearch.auth.user", USER)
                .put("elasticsearch.auth.password", PASSWORD);

        Map<String, String> config = builder.putAll(extraConnectorProperties)
                .buildOrThrow();

        queryRunner.createCatalog(catalogName, "elasticsearch", config);
    }

    private static void loadTpchTopic(RestHighLevelClient client, TestingTrinoClient trinoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        LOG.info("Running import for %s", table.getTableName());
        ElasticsearchLoader loader = new ElasticsearchLoader(client, table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession());
        loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = createElasticsearchQueryRunner(
                new ElasticsearchServer(ELASTICSEARCH_7_IMAGE),
                TpchTable.getTables(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                3);

        Logger log = Logger.get(ElasticsearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
