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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.trino.plugin.postgresql.TestingPostgreSqlServer.DEFAULT_IMAGE_NAME;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class PostgreSqlQueryRunner
{
    private PostgreSqlQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static Builder builder(TestingPostgreSqlServer server)
    {
        return new Builder()
                .addConnectorProperties(Map.of(
                        "connection-url", server.getJdbcUrl(),
                        "connection-user", server.getUser(),
                        "connection-password", server.getPassword(),
                        "postgresql.include-system-tables", "true"));
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private Consumer<QueryRunner> additionalSetup = _ -> {};

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog("postgresql")
                    .setSchema(TPCH_SCHEMA)
                    .build());
        }

        @Override
        @CanIgnoreReturnValue
        public Builder setAdditionalSetup(Consumer<QueryRunner> additionalSetup)
        {
            this.additionalSetup = requireNonNull(additionalSetup, "additionalSetup is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            super.setAdditionalSetup(runner -> {
                runner.installPlugin(new GeoPlugin()); // GeoPlugin needs to be installed early.

                additionalSetup.accept(runner);

                runner.installPlugin(new TpchPlugin());
                runner.createCatalog("tpch", "tpch");

                runner.installPlugin(new PostgreSqlPlugin());
                runner.createCatalog("postgresql", "postgresql", connectorProperties);

                copyTpchTables(runner, "tpch", TINY_SCHEMA_NAME, initialTables);
            });
            return super.build();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = builder(new TestingPostgreSqlServer(System.getProperty("testing.postgresql-image-name", DEFAULT_IMAGE_NAME), true))
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");

        Logger log = Logger.get(PostgreSqlQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
