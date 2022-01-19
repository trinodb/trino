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
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.SessionPropertyManager;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.kafka.TestingKafka;

import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class PinotQueryRunner
{
    public static final String PINOT_CATALOG = "pinot";

    private PinotQueryRunner() {}

    public static DistributedQueryRunner createPinotQueryRunner(Map<String, String> extraProperties, Map<String, String> extraPinotProperties, Optional<Module> extension)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new PinotPlugin(extension));
        queryRunner.createCatalog(PINOT_CATALOG, "pinot", extraPinotProperties);
        return queryRunner;
    }

    public static Session createSession(String schema)
    {
        return createSession(schema, new PinotConfig());
    }

    public static Session createSession(String schema, PinotConfig config)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(config);
        sessionPropertyManager.addConnectorSessionProperties(new CatalogName(PINOT_CATALOG), pinotSessionProperties.getSessionProperties());
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog(PINOT_CATALOG)
                .setSchema(schema)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingKafka kafka = TestingKafka.createWithSchemaRegistry();
        kafka.start();
        TestingPinotCluster pinot = new TestingPinotCluster(kafka.getNetwork(), false);
        pinot.start();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        Map<String, String> pinotProperties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", pinot.getControllerConnectString())
                .put("pinot.segments-per-split", "10")
                .put("pinot.request-timeout", "3m")
                .buildOrThrow();
        DistributedQueryRunner queryRunner = createPinotQueryRunner(properties, pinotProperties, Optional.empty());
        Thread.sleep(10);
        Logger log = Logger.get(PinotQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
