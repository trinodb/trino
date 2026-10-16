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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMemoryPageSourceProvider
{
    @Test
    void testReportsMemoryUsageToMemoryContext()
            throws IOException
    {
        MemoryPagesStore pagesStore = new MemoryPagesStore(new MemoryConfig());
        pagesStore.initialize(0);
        Page page = new Page(createLongSequenceBlock(0, 100));
        pagesStore.add(0L, page);
        MemoryPageSourceProvider pageSourceProvider = new MemoryPageSourceProvider(pagesStore, new MemoryConfig());

        AtomicLong reportedBytes = new AtomicLong(-1);
        try (ConnectorPageSource _ = pageSourceProvider.createPageSource(
                MemoryTransactionHandle.INSTANCE,
                SESSION,
                new MemorySplit(0, 0, 1, HostAddress.fromString("localhost:8080"), 100, OptionalLong.empty()),
                new MemoryTableHandle(0, new SchemaTableName("default", "test"), OptionalLong.empty(), OptionalDouble.empty()),
                Optional.empty(),
                ImmutableList.of(new MemoryColumnHandle(0, "value", BIGINT)),
                DynamicFilter.EMPTY,
                reportedBytes::set)) {
            assertThat(reportedBytes).hasValue(page.getRetainedSizeInBytes());
        }
    }
}
