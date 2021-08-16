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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class YugabyteQueryRunner
{
    private YugabyteQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createYugabyteQueryRunner(
            TestingYugabyteServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("cassandra.contact-points", server.getHost());
            connectorProperties.putIfAbsent("cassandra.native-protocol-port", Integer.toString(server.getPort()));
            connectorProperties.putIfAbsent("cassandra.allow-drop-table", "true");

            queryRunner.installPlugin(new CassandraPlugin());
            queryRunner.createCatalog("yugabyte", "cassandra", connectorProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("yugabyte")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createYugabyteQueryRunner(
                new TestingYugabyteServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("cassandra.session-refresh-interval", "30s"));

        Logger log = Logger.get(YugabyteQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
