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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
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

import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.io.Resources.getResource;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.elasticsearch.ElasticsearchServer.ELASTICSEARCH_7_IMAGE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class ElasticsearchQueryRunner
{
    private ElasticsearchQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.elasticsearch.client.RestClient", Level.OFF);
    }

    private static final Logger LOG = Logger.get(ElasticsearchQueryRunner.class);

    public static final String USER = "elastic_user";
    public static final String PASSWORD = "123456";
    private static final String TPCH_SCHEMA = "tpch";

    public static Builder builder(ElasticsearchServer server)
    {
        try {
            return new Builder(server)
                    .addConnectorProperties(ImmutableMap.<String, String>builder()
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
                            .put("elasticsearch.auth.password", PASSWORD)
                            .buildOrThrow());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final ElasticsearchServer server;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(ElasticsearchServer server)
        {
            super(testSessionBuilder()
                    .setCatalog("elasticsearch")
                    .setSchema(TPCH_SCHEMA)
                    .build());
            this.server = requireNonNull(server, "server is null");
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(requireNonNull(connectorProperties, "connectorProperties is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new JmxPlugin());
                queryRunner.createCatalog("jmx", "jmx");

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new ElasticsearchPlugin(new ElasticsearchConnectorFactory()));
                queryRunner.createCatalog("elasticsearch", "elasticsearch", connectorProperties);

                TestingTrinoClient trinoClient = queryRunner.getClient();

                LOG.info("Loading data...");
                long startTime = System.nanoTime();
                try (RestHighLevelClient client = server.getClient()) {
                    for (TpchTable<?> table : initialTables) {
                        loadTpchTopic(client, trinoClient, table);
                    }
                }
                LOG.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
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
        QueryRunner queryRunner = builder(new ElasticsearchServer(ELASTICSEARCH_7_IMAGE))
                .addExtraProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(ElasticsearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
