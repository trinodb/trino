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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.pinot.PinotTpchTables.createTpchTables;
import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_LATEST_IMAGE_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;

public class PinotQueryRunner
{
    public static final String PINOT_CATALOG = "pinot";

    private PinotQueryRunner() {}

    public static QueryRunner createPinotQueryRunner(
            TestingKafka kafka,
            TestingPinotCluster pinot,
            Map<String, String> extraProperties,
            Map<String, String> extraPinotProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new PinotPlugin(Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort(), pinot.getServerGrpcHostAndPort())))));
        extraPinotProperties = new HashMap<>(ImmutableMap.copyOf(extraPinotProperties));
        extraPinotProperties.put("pinot.controller-urls", pinot.getControllerConnectString());
        queryRunner.createCatalog(PINOT_CATALOG, "pinot", extraPinotProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        createTpchTables(kafka, pinot, queryRunner, tables);

        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(PINOT_CATALOG)
                .setSchema("default")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingKafka kafka = TestingKafka.createWithSchemaRegistry();
        kafka.start();
        TestingPinotCluster pinot = new TestingPinotCluster(kafka.getNetwork(), false, PINOT_LATEST_IMAGE_NAME);
        pinot.start();
        QueryRunner queryRunner = createPinotQueryRunner(
                kafka,
                pinot,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("pinot.segments-per-split", "10"),
                Set.of(REGION, NATION, ORDERS, CUSTOMER));
        Thread.sleep(10);
        Logger log = Logger.get(PinotQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
