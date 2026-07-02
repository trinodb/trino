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
package io.trino.filesystem.manager;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.cache.CacheSplitAffinityProvider;
import io.trino.filesystem.cache.NoopSplitAffinityProvider;
import io.trino.filesystem.cache.SplitAffinityProvider;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.ConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestFileSystemModule
{
    @Test
    public void testSplitAffinityProviderDefaultsToNoopWithoutCache()
    {
        Injector injector = bootstrap(true, false, ImmutableMap.of("fs.s3.enabled", "true"));
        assertThat(injector.getInstance(SplitAffinityProvider.class))
                .isInstanceOf(NoopSplitAffinityProvider.class);
    }

    @Test
    public void testSplitAffinityProviderUsesCacheWhenCoordinatorMetadataCacheEnabled()
    {
        // Iceberg's default scenario: iceberg.metadata-cache.enabled=true installs the
        // MemoryFileSystemCache on the coordinator. The affinity provider must engage so
        // that two queries reading the same Iceberg manifest are routed to the same worker,
        // preserving the per-worker JVM cache hit ratio.
        Injector injector = bootstrap(true, true, ImmutableMap.of("fs.s3.enabled", "true"));
        assertThat(injector.getInstance(SplitAffinityProvider.class))
                .isInstanceOf(CacheSplitAffinityProvider.class);
    }

    @Test
    public void testSplitAffinityProviderRemainsNoopOnWorker()
    {
        // The consistent-hash provider is only useful on the coordinator, which is where
        // splits are generated. Workers must always resolve to the no-op provider regardless
        // of the cache flags.
        Injector injector = bootstrap(false, true, ImmutableMap.of("fs.s3.enabled", "true"));
        assertThat(injector.getInstance(SplitAffinityProvider.class))
                .isInstanceOf(NoopSplitAffinityProvider.class);
    }

    private static Injector bootstrap(boolean isCoordinator, boolean coordinatorFileCaching, Map<String, String> properties)
    {
        ConnectorContext context = new TestingConnectorContext(isCoordinator);
        Module extraBindings = binder -> {
            binder.bind(CatalogName.class).toInstance(new CatalogName("test"));
            binder.bind(OpenTelemetry.class).toInstance(OpenTelemetry.noop());
            binder.bind(Tracer.class).toInstance(OpenTelemetry.noop().getTracer("test"));
        };
        return new Bootstrap(
                extraBindings,
                new FileSystemModule("test", context, coordinatorFileCaching))
                .doNotInitializeLogging()
                .quiet()
                .setRequiredConfigurationProperties(properties)
                .initialize();
    }

    private static final class TestingConnectorContext
            implements ConnectorContext
    {
        private final Node node;

        TestingConnectorContext(boolean isCoordinator)
        {
            this.node = new TestingNode(isCoordinator);
        }

        @Override
        public Node getCurrentNode()
        {
            return node;
        }

        @Override
        public Tracer getTracer()
        {
            return OpenTelemetry.noop().getTracer("test");
        }
    }

    private static final class TestingNode
            implements Node
    {
        private final boolean coordinator;

        TestingNode(boolean coordinator)
        {
            this.coordinator = coordinator;
        }

        @Override
        public String getHost()
        {
            return "localhost";
        }

        @Override
        public HostAddress getHostAndPort()
        {
            return HostAddress.fromParts("localhost", 8080);
        }

        @Override
        public String getNodeIdentifier()
        {
            return "test";
        }

        @Override
        public String getVersion()
        {
            return "test";
        }

        @Override
        public boolean isCoordinator()
        {
            return coordinator;
        }
    }
}
