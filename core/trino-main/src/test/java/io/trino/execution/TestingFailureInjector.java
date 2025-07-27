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

import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableCache;
import io.trino.spi.ErrorType;

import java.util.Optional;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TestingFailureInjector
        implements FailureInjector
{
    private final NonEvictableCache<Key, InjectedFailure> failures;
    private final Duration requestTimeout;

    @Inject
    public TestingFailureInjector(TestingFailureInjectionConfig config)
    {
        this(
                config.getExpirationPeriod(),
                config.getRequestTimeout());
    }

    public TestingFailureInjector(Duration expirationPeriod, Duration requestTimeout)
    {
        failures = buildNonEvictableCache(CacheBuilder.newBuilder()
                .expireAfterWrite(expirationPeriod.toMillis(), MILLISECONDS));
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
    }

    @Override
    public void injectTaskFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId,
            InjectedFailureType injectionType,
            Optional<ErrorType> errorType)
    {
        failures.put(new Key(traceToken, stageId, partitionId, attemptId), new InjectedFailure(injectionType, errorType));
    }

    @Override
    public Optional<InjectedFailure> getInjectedFailure(
            String traceToken,
            int stageId,
            int partitionId,
            int attemptId)
    {
        if (failures.size() == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(failures.getIfPresent(new Key(traceToken, stageId, partitionId, attemptId)));
    }

    @Override
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    private record Key(String traceToken, int stageId, int partitionId, int attemptId)
    {
        private Key
        {
            requireNonNull(traceToken, "traceToken is null");
        }
    }
}
