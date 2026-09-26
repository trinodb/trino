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
package io.trino.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeSourceHandle.SourceFile;
import io.trino.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.EXCHANGE_DATA_UNRECOVERABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileSystemExchangeSource
{
    @Test
    public void testIsBlockedNonCancellable()
    {
        try (FileSystemExchangeSource source = new FileSystemExchangeSource(
                new LocalFileSystemExchangeStorage(),
                new FileSystemExchangeStats(),
                1024,
                2,
                1)) {
            CompletableFuture<Void> first = source.isBlocked();
            CompletableFuture<Void> second = source.isBlocked();
            assertThat(first)
                    .isNotDone()
                    .isNotCancelled();
            assertThat(second)
                    .isNotDone()
                    .isNotCancelled();

            first.cancel(true);
            assertThat(first)
                    .isDone()
                    .isCancelled();
            assertThat(second)
                    .isNotDone()
                    .isNotCancelled();

            CompletableFuture<Void> third = source.isBlocked();
            assertThat(third)
                    .isNotDone()
                    .isNotCancelled();
        }
    }

    @Test
    public void testMissingFileIsUnrecoverable()
    {
        ExchangeId exchangeId = new ExchangeId("exchange");
        Path missingFile = Path.of(System.getProperty("java.io.tmpdir"), "missing-exchange-" + System.nanoTime(), "0_0_0.data");
        try (FileSystemExchangeSource source = new FileSystemExchangeSource(
                new LocalFileSystemExchangeStorage(),
                new FileSystemExchangeStats(),
                1024,
                1,
                1)) {
            source.setOutputSelector(ExchangeSourceOutputSelector.builder(ImmutableSet.of(exchangeId))
                    .include(exchangeId, 0, 0)
                    .setPartitionCount(exchangeId, 1)
                    .setFinal()
                    .build());
            source.addSourceHandles(ImmutableList.of(new FileSystemExchangeSourceHandle(
                    exchangeId,
                    0,
                    ImmutableList.of(new SourceFile(missingFile.toUri().toString(), 16, 0, 0)))));
            source.noMoreSourceHandles();

            assertThatThrownBy(source::read)
                    .isInstanceOfSatisfying(TrinoException.class, exception ->
                            assertThat(exception.getErrorCode()).isEqualTo(EXCHANGE_DATA_UNRECOVERABLE.toErrorCode()))
                    .hasMessage("Exchange source data is gone");
        }
    }
}
