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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.kafka.TestingKafka;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.pinot.PinotTpchTables.createTpchTables;
import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_LATEST_IMAGE_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;

public final class PinotQueryRunner
{
    private PinotQueryRunner() {}

    public static final String PINOT_CATALOG = "pinot";

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private TestingKafka kafka;
        private TestingPinotCluster pinot;
        private final ImmutableMap.Builder<String, String> pinotProperties = ImmutableMap.builder();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(PINOT_CATALOG)
                    .setSchema("default")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder setKafka(TestingKafka kafka)
        {
            this.kafka = kafka;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setPinot(TestingPinotCluster pinot)
        {
            this.pinot = pinot;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addPinotProperties(Map<String, String> pinotProperties)
        {
            this.pinotProperties.putAll(pinotProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addPinotProperty(String key, String value)
        {
            pinotProperties.put(key, value);
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
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new PinotPlugin(Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort(), pinot.getServerGrpcHostAndPort())))));
                Map<String, String> extraPinotProperties = new HashMap<>(pinotProperties.buildOrThrow());
                extraPinotProperties.put("pinot.controller-urls", pinot.getControllerConnectString());
                extraPinotProperties.put("pinot.metadata-expiry", "5s");
                queryRunner.createCatalog(PINOT_CATALOG, "pinot", extraPinotProperties);

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");
                createTpchTables(kafka, pinot, queryRunner, initialTables);
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
            return queryRunner;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingKafka kafka = TestingKafka.createWithSchemaRegistry();
        kafka.start();
        TestingPinotCluster pinot = new TestingPinotCluster(PINOT_LATEST_IMAGE_NAME, kafka.getNetwork(), false);
        pinot.start();
        QueryRunner queryRunner = builder()
                .setKafka(kafka)
                .setPinot(pinot)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addPinotProperty("pinot.segments-per-split", "10")
                .setInitialTables(List.of(REGION, NATION, ORDERS, CUSTOMER))
                .build();

        Logger log = Logger.get(PinotQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
