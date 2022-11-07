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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import io.airlift.log.Logger;

import java.util.concurrent.ThreadLocalRandom;

public class BackoffRetryPolicy
        implements RetryPolicy
{
    private static final Logger log = Logger.get(BackoffRetryPolicy.class);

    private final String logPrefix;

    public BackoffRetryPolicy(DriverContext context, String profileName)
    {
        this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
    }

    @Override
    public RetryDecision onReadTimeout(Request request, ConsistencyLevel consistencyLevel, int blockFor, int received, boolean dataPresent, int retryCount)
    {
        RetryDecision decision =
                (retryCount == 0 && received >= blockFor && !dataPresent)
                        ? RetryDecision.RETRY_SAME
                        : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_SAME) {
            log.debug(
                    "[%s] Retrying on read timeout on same host (consistency: %s, required responses: %s, received responses: %s, data retrieved: %s, retries: %s)",
                    logPrefix,
                    consistencyLevel,
                    blockFor,
                    received,
                    false,
                    retryCount);
        }

        return decision;
    }

    @Override
    public RetryDecision onWriteTimeout(Request request, ConsistencyLevel consistencyLevel, WriteType writeType, int blockFor, int received, int retryCount)
    {
        RetryDecision decision =
                (retryCount == 0 && writeType == DefaultWriteType.BATCH_LOG)
                        ? RetryDecision.RETRY_SAME
                        : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_SAME && log.isDebugEnabled()) {
            log.debug(
                    "[%s] Retrying on write timeout on same host (consistency: %s, write type: %s, required acknowledgments: %s, received acknowledgments: %s, retries: %s)",
                    logPrefix,
                    consistencyLevel,
                    writeType,
                    blockFor,
                    received,
                    retryCount);
        }
        return decision;
    }

    @Override
    public RetryDecision onUnavailable(Request request, ConsistencyLevel consistencyLevel, int required, int alive, int retries)
    {
        if (retries >= 10) {
            return RetryDecision.RETHROW;
        }

        try {
            int jitter = ThreadLocalRandom.current().nextInt(100);
            int delay = (100 * (retries + 1)) + jitter;
            Thread.sleep(delay);
            return RetryDecision.RETRY_SAME;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return RetryDecision.RETHROW;
        }
    }

    @Override
    public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount)
    {
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onErrorResponse(Request request, CoordinatorException error, int retryCount)
    {
        log.debug(error, "[%s] Retrying on node error on next host (retries: %s)", logPrefix, retryCount);
        return RetryDecision.RETRY_NEXT;
    }

    @Override
    public void close() {}
}
