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

import io.trino.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

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
}
