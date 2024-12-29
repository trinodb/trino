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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logging;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class LakehouseQueryRunner
{
    static {
        Logging.initialize();
    }

    private LakehouseQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final ImmutableMap.Builder<String, String> lakehouseProperties = ImmutableMap.builder();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("lakehouse")
                    .setSchema("tpch")
                    .build());
        }

        public Builder addLakehouseProperty(String key, String value)
        {
            lakehouseProperties.put(key, value);
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                // needed for $iceberg_theta_stat function
                queryRunner.installPlugin(new IcebergPlugin());

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new TpcdsPlugin());
                queryRunner.createCatalog("tpcds", "tpcds");

                queryRunner.installPlugin(new LakehousePlugin());
                queryRunner.createCatalog("lakehouse", "lakehouse", lakehouseProperties.buildOrThrow());

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }
}
