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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class KuduQueryRunnerFactory
{
    private KuduQueryRunnerFactory() {}

    public static Builder builder(TestingKuduServer kuduServer)
    {
        return new Builder(kuduServer);
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final TestingKuduServer kuduServer;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private Optional<String> kuduSchemaEmulationPrefix = Optional.empty();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(TestingKuduServer kuduServer)
        {
            super(testSessionBuilder()
                    .setCatalog("kudu")
                    .setSchema("default")
                    .build());
            this.kuduServer = requireNonNull(kuduServer, "kuduServer is null");
        }

        @CanIgnoreReturnValue
        public Builder setKuduSchemaEmulationPrefix(Optional<String> kuduSchemaEmulationPrefix)
        {
            this.kuduSchemaEmulationPrefix = requireNonNull(kuduSchemaEmulationPrefix, "kuduSchemaEmulationPrefix is null");
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(List<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(initialTables);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            String kuduSchema = kuduSchemaEmulationPrefix.isPresent() ? "tpch" : "default";
            amendSession(sessionBuilder -> sessionBuilder.setSchema(kuduSchema));
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                if (kuduSchemaEmulationPrefix.isPresent()) {
                    addConnectorProperty("kudu.schema-emulation.enabled", "true");
                    addConnectorProperty("kudu.schema-emulation.prefix", kuduSchemaEmulationPrefix.get());
                    addConnectorProperty("kudu.client.master-addresses", kuduServer.getMasterAddress().toString());
                }
                else {
                    addConnectorProperty("kudu.schema-emulation.enabled", "false");
                    addConnectorProperty("kudu.client.master-addresses", kuduServer.getMasterAddress().toString());
                }

                queryRunner.installPlugin(new KuduPlugin());
                queryRunner.createCatalog("kudu", "kudu", connectorProperties);

                if (kuduSchemaEmulationPrefix.isPresent()) {
                    queryRunner.execute("DROP SCHEMA IF EXISTS " + kuduSchema);
                    queryRunner.execute("CREATE SCHEMA " + kuduSchema);
                }

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = builder(new TestingKuduServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(KuduQueryRunnerFactory.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
