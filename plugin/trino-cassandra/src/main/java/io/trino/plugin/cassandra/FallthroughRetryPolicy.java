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
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

/**
 * A retry policy that never retries (nor ignores).
 * <p/>
 * All of the methods of this retry policy unconditionally return {@link RetryDecision#RETHROW}.
 * If this policy is used, retry logic will have to be implemented in business code.
 */
public class FallthroughRetryPolicy
        implements RetryPolicy
{
    // Required by Cassandra driver library for instantiation
    public FallthroughRetryPolicy(DriverContext context, String profileName) {}

    @Override
    public RetryDecision onReadTimeout(Request request, ConsistencyLevel consistencyLevel, int blockFor, int received, boolean dataPresent, int retryCount)
    {
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onWriteTimeout(Request request, ConsistencyLevel consistencyLevel, WriteType writeType, int blockFor, int received, int retryCount)
    {
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onUnavailable(Request request, ConsistencyLevel consistencyLevel, int required, int alive, int retries)
    {
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount)
    {
        return RetryDecision.RETHROW;
    }

    @Override
    public RetryDecision onErrorResponse(Request request, CoordinatorException error, int retryCount)
    {
        return RetryDecision.RETHROW;
    }

    @Override
    public void close() {}
}
