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
package io.trino.server.protocol;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.server.ExternalUriInfo;

/**
 * Drains the results of a dispatched query, one token at a time. Nothing in the contract depends
 * on the caller serving each token as a separate HTTP request.
 */
public interface ProtocolQuery
{
    /**
     * Returns the results at {@code token}, waiting up to {@code wait} for them to become
     * available. Requesting a token twice replays it; requesting {@code token + 1} acknowledges
     * the previous batch and frees its buffers.
     */
    ListenableFuture<QueryResultsResponse> waitForResults(long token, ExternalUriInfo externalUriInfo, Duration wait);

    /**
     * Signals that the client has consumed everything it asked for, so a query whose results are
     * fully delivered can move out of the FINISHING state without waiting for another poll.
     */
    void markResultsConsumedIfReady();

    void cancel();

    /**
     * Releases the result buffers. The query itself keeps running unless it is also cancelled.
     */
    void dispose();
}
