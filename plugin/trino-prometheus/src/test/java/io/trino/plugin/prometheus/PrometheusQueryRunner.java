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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.base.util.Closables;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.prometheus.MetadataUtil.METRIC_CODEC;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class PrometheusQueryRunner
{
    private PrometheusQueryRunner() {}

    public static Builder builder(PrometheusServer prometheusServer)
    {
        return new Builder()
                .addConnectorProperty("prometheus.uri", prometheusServer.getUri().toString());
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("prometheus")
                    .setSchema("default")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new PrometheusPlugin());
                queryRunner.createCatalog("prometheus", "prometheus", connectorProperties);
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
            return queryRunner;
        }
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
        QueryRunner queryRunner = builder(new PrometheusServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();
        Logger log = Logger.get(PrometheusQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
