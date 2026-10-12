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

public interface SplitRunner
        extends Closeable
{
    int getPipelineId();

    Span getPipelineSpan();

    boolean isFinished();

    ListenableFuture<Void> processFor(Duration duration);

    // Reports what the last processFor call blocked on.
    default BlockedReason getBlockedReason()
    {
        return BlockedReason.OPERATOR;
    }

    String getInfo();

    @Override
    void close();

    // Declared in increasing precedence when several operators block at once.
    enum BlockedReason
    {
        // The pipeline source operator waits for input.
        SOURCE,
        // An operator between the source and the sink waits, such as a join probe waiting for its build.
        OPERATOR,
        // The pipeline sink waits for its consumer.
        OUTPUT,
        // An operator waits for memory or for revocation.
        MEMORY,
    }
}
