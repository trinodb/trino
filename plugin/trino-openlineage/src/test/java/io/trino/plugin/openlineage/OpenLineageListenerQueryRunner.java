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
import io.trino.plugin.base.evenlistener.TestingEventListenerContext;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

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

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            super.setEventListener(new OpenLineageListenerFactory().create(listenerProperties, new TestingEventListenerContext()));
            DistributedQueryRunner queryRunner = super.build();
            try {
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
