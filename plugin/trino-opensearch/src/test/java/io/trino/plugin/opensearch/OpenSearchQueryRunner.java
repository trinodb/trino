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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
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
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.opensearch.OpenSearchServer.OPENSEARCH_IMAGE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class OpenSearchQueryRunner
{
    private OpenSearchQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.opensearch.client.RestClient", Level.OFF);
    }

    private static final Logger LOG = Logger.get(OpenSearchQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    public static Builder builder(HostAndPort address)
    {
        return new Builder(address);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final HostAndPort address;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(HostAndPort address)
        {
            super(testSessionBuilder()
                    .setCatalog("opensearch")
                    .setSchema(TPCH_SCHEMA)
                    .build());

            connectorProperties.put("opensearch.host", address.getHost());
            connectorProperties.put("opensearch.port", Integer.toString(address.getPort()));
            // Node discovery relies on the publish_address exposed via the OpenSearch API
            // This doesn't work well within a docker environment that maps OpenSearch port to a random public port
            connectorProperties.put("opensearch.ignore-publish-address", "true");
            connectorProperties.put("opensearch.default-schema-name", TPCH_SCHEMA);
            connectorProperties.put("opensearch.scroll-size", "1000");
            connectorProperties.put("opensearch.scroll-timeout", "1m");
            connectorProperties.put("opensearch.request-timeout", "2m");

            this.address = requireNonNull(address, "address is null");
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

                queryRunner.installPlugin(new OpenSearchPlugin(new OpenSearchConnectorFactory()));
                queryRunner.createCatalog("opensearch", "opensearch", connectorProperties);

                LOG.info("Loading data...");
                long startTime = System.nanoTime();
                try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create(address.toString())))) {
                    for (TpchTable<?> table : initialTables) {
                        loadTpchTopic(client, queryRunner.getClient(), table);
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
        OpenSearchLoader loader = new OpenSearchLoader(client, table.getTableName().toLowerCase(ENGLISH), trinoClient.getServer(), trinoClient.getDefaultSession());
        loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
        LOG.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = builder(new OpenSearchServer(OPENSEARCH_IMAGE, false, ImmutableMap.of()).getAddress())
                .addExtraProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(OpenSearchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
