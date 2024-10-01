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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_CUSTOMER;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_LINEITEM;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_NATION;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_ORDERS;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_REGION;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class PulsarQueryRunner
{
    private PulsarQueryRunner()
    {
    }

    public static DistributedQueryRunner createPulsarQueryRunner(PulsarServer testServer, Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession())
                    .setExtraProperties(extraProperties)
                    .build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            Map<String, String> connectorProperties = new HashMap<>();
            connectorProperties.put("pulsar.web-service-url", testServer.getPulsarAdminUrl());
            connectorProperties.put("pulsar.zookeeper-uri", testServer.getZKUrl());
            connectorProperties.put("pulsar.hide-internal-columns", "true");
            queryRunner.installPlugin(new PulsarPlugin());
            queryRunner.createCatalog("pulsar", "pulsar", connectorProperties);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
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
        DistributedQueryRunner queryRunner = createPulsarQueryRunner(
                pulsarServer,
                ImmutableMap.of("http-server.http.port", "8080"));

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
