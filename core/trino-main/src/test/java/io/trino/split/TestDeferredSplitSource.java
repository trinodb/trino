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

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.split.MockSplitSource.Action.FINISH;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestDeferredSplitSource
{
    @Test
    void testDoesNotCreateSplitsBeforeDiscoveryCompletes()
    {
        CompletableFuture<SplitSource> delegate = new CompletableFuture<>();
        DeferredSplitSource source = new DeferredSplitSource(TEST_CATALOG_HANDLE, delegate);

        ListenableFuture<SplitSource.SplitBatch> batch = source.getNextBatch(10);

        assertThat(source.isSplitSourceCreationDeferred()).isTrue();
        assertThat(batch).isNotDone();

        MockSplitSource mock = new MockSplitSource()
                .setBatchSize(1)
                .increaseAvailableSplits(1)
                .atSplitCompletion(FINISH);
        delegate.complete(mock);

        assertThat(getFutureValue(batch).getSplits()).hasSize(1);
        assertThat(source.isSplitSourceCreationDeferred()).isFalse();
    }

    @Test
    void testPreservesSplitSourceCreationFailure()
    {
        IllegalArgumentException failure = new IllegalArgumentException("Invalid connector metadata");
        CompletableFuture<SplitSource> delegate = CompletableFuture.completedFuture(true).thenApply(_ -> {
            throw failure;
        });
        DeferredSplitSource source = new DeferredSplitSource(TEST_CATALOG_HANDLE, delegate);

        assertThatThrownBy(() -> getFutureValue(source.getNextBatch(10))).isSameAs(failure);
    }
}
