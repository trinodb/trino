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
import com.google.common.net.HostAndPort;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class PinotLocalQueryRunner
{
    private PinotLocalQueryRunner()
    {
    }

    public static DistributedQueryRunner createPinotQueryRunner(Map<String, String> extraProperties, Map<String, String> extraPinotProperties, Optional<Module> extension)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new PinotPlugin(extension));
        queryRunner.createCatalog("pinot", "pinot", extraPinotProperties);
        return queryRunner;
    }

    public static Session createSession(String schema)
    {
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog("pinot")
                .setSchema(schema)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        Map<String, String> pinotProperties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", "localhost:33453")
                .put("pinot.segments-per-split", "10")
                .put("pinot.request-timeout", "3m")
                .build();
        //DistributedQueryRunner queryRunner = createPinotQueryRunner(properties, pinotProperties, Optional.empty());
        DistributedQueryRunner queryRunner = createPinotQueryRunner(properties, pinotProperties, Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                .toInstance(new TestingPinotHostMapper(HostAndPort.fromParts("localhost", 33459), HostAndPort.fromParts("localhost", 33465)))));
        Thread.sleep(10);
        Logger log = Logger.get(PinotLocalQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
