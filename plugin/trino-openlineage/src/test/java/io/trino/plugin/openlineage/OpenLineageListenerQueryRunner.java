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
package io.trino.plugin.openlineage;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OpenLineageListenerQueryRunner
{
    public static final String CATALOG = "marquez";
    public static final String SCHEMA = "default";

    private OpenLineageListenerQueryRunner() {}

    public static QueryRunner createOpenLineageRunner(Map<String, String> listenerProperties)
            throws Exception
    {
        QueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner
                    .builder(createSession())
                    .setEventListener(new OpenLineageListenerFactory().create(listenerProperties))
                    .build();
            // catalog used for output data
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog(CATALOG, "memory");

            // catalog used for input data
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

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
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        MarquezServer server = new MarquezServer();

        Map<String, String> config = ImmutableMap.of(
                "openlineage-event-listener.transport.type", "HTTP",
                "openlineage-event-listener.transport.url", server.getMarquezUri().toString(),
                "openlineage-event-listener.trino.uri", "http://trino-query-runner:1337");

        QueryRunner queryRunner = createOpenLineageRunner(config);
        Logger log = Logger.get(OpenLineageListenerQueryRunner.class);
        log.info("======== SERVER RUNNING: %s ========", queryRunner.getCoordinator().getBaseUrl());

        if (server.getMarquezWebUIUri().isPresent()) {
            log.info("======== MARQUEZ UI RUNNING: %s ========", server.getMarquezWebUIUri().get());
        }
    }
}
