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
package io.trino.plugin.pulsar;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_CUSTOMER;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_LINEITEM;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_NATION;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_ORDERS;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_REGION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PulsarQueryRunner
{
    private PulsarQueryRunner()
    {
    }

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.pulsar", Level.OFF);
    }

    private static final Logger log = Logger.get(PulsarQueryRunner.class);

    public static Builder builder(PulsarServer server)
    {
        return new Builder()
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("pulsar.web-service-url", server.getPulsarAdminUrl())
                        .put("pulsar.metadata-uri", server.getZKUrl())
                        .put("pulsar.hide-internal-columns", "true")
                        .buildOrThrow());
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("pulsar")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner = DistributedQueryRunner.builder(createSession())
                    .build();

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new PulsarPlugin());
                queryRunner.createCatalog("pulsar", "pulsar", connectorProperties);

                populateTables(queryRunner, initialTables);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static void populateTables(QueryRunner queryRunner, List<TpchTable<?>> tables)
    {
        log.info("Loading data...");
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            long start = System.nanoTime();
            log.info("Running import for %s", table.getTableName());
            queryRunner.execute(format("INSERT INTO %1$s SELECT * FROM tpch.tiny.%1$s", table.getTableName()));
            log.info("Imported %s in %s", table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
        }
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("pulsar")
                .setSchema("public/default")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        PulsarServer pulsarServer = new PulsarServer(PulsarServer.DEFAULT_IMAGE_NAME);
        QueryRunner queryRunner = builder(pulsarServer)
                .setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(PulsarQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        pulsarServer.copyAndIngestTpchData(queryRunner.execute(SELECT_FROM_CUSTOMER), PulsarServer.CUSTOMER, PulsarServer.Customer.class, 2);
        pulsarServer.copyAndIngestTpchData(queryRunner.execute(SELECT_FROM_ORDERS), PulsarServer.ORDERS, PulsarServer.Orders.class, 3);
        pulsarServer.copyAndIngestTpchData(queryRunner.execute(SELECT_FROM_LINEITEM), PulsarServer.LINEITEM, PulsarServer.LineItem.class, 4);
        pulsarServer.copyAndIngestTpchData(queryRunner.execute(SELECT_FROM_NATION), PulsarServer.NATION, PulsarServer.Nation.class, 1);
        pulsarServer.copyAndIngestTpchData(queryRunner.execute(SELECT_FROM_REGION), PulsarServer.REGION, PulsarServer.Region.class, 1);
        log.info("======== Done ========");
    }
}
