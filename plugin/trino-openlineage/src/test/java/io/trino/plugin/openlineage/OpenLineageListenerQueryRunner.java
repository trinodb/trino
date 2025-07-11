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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.plugin.base.evenlistener.TestingEventListenerContext;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OpenLineageListenerQueryRunner
{
    public static final String CATALOG = "marquez";
    public static final String SCHEMA = "default";

    private OpenLineageListenerQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> listenerProperties = new HashMap<>();
        private Supplier<EventListener> eventListenerSupplier = () ->
                new OpenLineageListenerFactory().create(listenerProperties, new TestingEventListenerContext());

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema(SCHEMA)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addListenerProperty(String key, String value)
        {
            this.listenerProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setCustomEventListener(EventListener eventListener)
        {
            this.eventListenerSupplier = () -> eventListener;
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            super.setEventListener(eventListenerSupplier.get());
            DistributedQueryRunner queryRunner = super.build();
            try {
                // catalog used for output data
                queryRunner.installPlugin(new MemoryPlugin());
                queryRunner.createCatalog(CATALOG, "memory");

                // catalog used for input data
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                // catalog used for input data
                queryRunner.installPlugin(new TpcdsPlugin());
                queryRunner.createCatalog("tpcds", "tpcds");

                // catalog used for materialized views
                queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.create()));
                queryRunner.createCatalog("mock", "mock");

                // catalog used for deletes and merges
                queryRunner.installPlugin(new BlackHolePlugin());
                queryRunner.createCatalog("blackhole", "blackhole");

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        MarquezServer server = new MarquezServer();

        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addListenerProperty("openlineage-event-listener.transport.type", "HTTP")
                .addListenerProperty("openlineage-event-listener.transport.url", server.getMarquezUri().toString())
                .addListenerProperty("openlineage-event-listener.trino.uri", "http://localhost:8080")
                .build();
        Logger log = Logger.get(OpenLineageListenerQueryRunner.class);
        log.info("======== SERVER RUNNING: %s ========", queryRunner.getCoordinator().getBaseUrl());

        log.info("======== MARQUEZ UI RUNNING: %s ========", server.getMarquezWebUIUri());
    }
}
