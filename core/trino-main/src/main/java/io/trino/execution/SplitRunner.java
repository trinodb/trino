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
package io.trino.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;

import java.io.Closeable;
import java.util.OptionalInt;

public interface SplitRunner
        extends Closeable
{
    int getPipelineId();

    Span getPipelineSpan();

    boolean isFinished();

    ListenableFuture<Void> processFor(Duration duration);

    /// When [#processFor] last returned an unfinished blocked future, the id of the pipeline whose
    /// output this split is waiting on (e.g. the build pipeline a hash-join probe reads from), if
    /// known. The scheduler donates priority to that pipeline while this split is blocked so the
    /// dependency clears sooner. Empty when the block has no identifiable producer pipeline.
    default OptionalInt getBlockedProducerPipeline()
    {
        return OptionalInt.empty();
    }

    String getInfo();

    @Override
    void close();
}
