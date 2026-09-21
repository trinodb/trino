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
package io.trino.split;

import io.trino.connector.CatalogServiceProvider;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.MemoryContext;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPageSourceManager
{
    @Test
    public void testSharedMemoryReleasedWithLastReference()
    {
        AtomicReference<MemoryContext> sharedMemoryContext = new AtomicReference<>();
        AggregatedMemoryContext scanMemoryContext = newSimpleAggregatedMemoryContext();
        PageSourceProvider provider = createPageSourceProvider(sharedMemoryContext, scanMemoryContext);

        provider.retain();
        provider.retain();
        sharedMemoryContext.get().setBytes(1024);
        assertThat(scanMemoryContext.getBytes()).isEqualTo(1024);

        provider.release();
        provider.release();
        assertThat(scanMemoryContext.getBytes()).isEqualTo(1024);

        // the reference held by the operator factory
        provider.release();
        assertThat(scanMemoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testReleaseWithoutReference()
    {
        PageSourceProvider provider = createPageSourceProvider(new AtomicReference<>(), newSimpleAggregatedMemoryContext());

        provider.release();
        assertThatThrownBy(provider::release)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Reference has already been freed");
    }

    private static PageSourceProvider createPageSourceProvider(AtomicReference<MemoryContext> sharedMemoryContext, AggregatedMemoryContext scanMemoryContext)
    {
        ConnectorPageSourceProviderFactory factory = memoryContext -> {
            sharedMemoryContext.set(memoryContext);
            return new ConnectorPageSourceProvider() {};
        };
        return new PageSourceManager(CatalogServiceProvider.singleton(TEST_CATALOG_HANDLE, factory))
                .createPageSourceProvider(TEST_CATALOG_HANDLE, scanMemoryContext);
    }
}
