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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class PrometheusQueryRunner
{
    private PrometheusQueryRunner() {}

    public static DistributedQueryRunner createPrometheusQueryRunner(PrometheusServer server, Map<String, String> extraProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).setExtraProperties(extraProperties).build();

            queryRunner.installPlugin(new PrometheusPlugin());
            Map<String, String> properties = ImmutableMap.of(
                    "prometheus.uri", server.getUri().toString());
            queryRunner.createCatalog("prometheus", "prometheus", properties);
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
                .setCatalog("prometheus")
                .setSchema("default")
                .build();
    }

    public static PrometheusClient createPrometheusClient(PrometheusServer server)
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(server.getUri());
        config.setQueryChunkSizeDuration(new Duration(1, DAYS));
        config.setMaxQueryRangeDuration(new Duration(21, DAYS));
        config.setCacheDuration(new Duration(30, SECONDS));
        config.setReadTimeout(new Duration(10, SECONDS));
        return new PrometheusClient(config, METRIC_CODEC, TESTING_TYPE_MANAGER);
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createPrometheusQueryRunner(new PrometheusServer(), ImmutableMap.of("http-server.http.port", "8080"));
        Thread.sleep(10);
        Logger log = Logger.get(PrometheusQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
