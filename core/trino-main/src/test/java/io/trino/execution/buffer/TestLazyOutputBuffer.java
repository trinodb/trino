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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.exchange.ExchangeManagerConfig;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.spi.QueryId;
import org.junit.jupiter.api.Test;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLazyOutputBuffer
{
    @Test
    public void testUsesExternalStorageBeforeInitialization()
    {
        LazyOutputBuffer buffer = createUninitializedBuffer();
        assertThatThrownBy(buffer::usesExternalStorage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Buffer has not been initialized");
    }

    private static LazyOutputBuffer createUninitializedBuffer()
    {
        return new LazyOutputBuffer(
                new TaskId(new StageId(new QueryId("query"), 0), 0, 0),
                0,
                directExecutor(),
                DataSize.of(1, MEGABYTE),
                DataSize.of(1, MEGABYTE),
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), TestLazyOutputBuffer.class.getSimpleName()),
                () -> {},
                new ExchangeManagerRegistry(OpenTelemetry.noop(), Tracing.noopTracer(), new SecretsResolver(ImmutableMap.of()), new ExchangeManagerConfig()));
    }
}
