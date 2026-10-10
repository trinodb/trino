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
package io.trino.plugin.starrocks;

import io.trino.plugin.base.util.Closables;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class StarRocksQueryRunner
{
    private StarRocksQueryRunner() {}

    public static Builder builder(TestingStarRocksEnvironment environment)
    {
        return new Builder(environment);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final TestingStarRocksEnvironment environment;
        private final Map<String, String> connectorProperties = new HashMap<>();

        private Builder(TestingStarRocksEnvironment environment)
        {
            super(testSessionBuilder()
                    .setCatalog("starrocks")
                    .setSchema("tiny")
                    .build());
            this.environment = requireNonNull(environment, "environment is null");
        }

        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new StarRocksPlugin(environment.createModule()));

                Map<String, String> properties = new HashMap<>();
                properties.put("starrocks.jdbc-url", "jdbc:starrocks://127.0.0.1:9030");
                properties.putAll(connectorProperties);

                queryRunner.createCatalog("starrocks", "starrocks", properties);
                return queryRunner;
            }
            catch (Throwable t) {
                Closables.closeAllSuppress(t, queryRunner);
                throw t;
            }
        }
    }
}
