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

import io.airlift.units.Duration;
import io.trino.spi.ErrorType;

import java.util.Optional;

public class NoOpFailureInjector
        implements FailureInjector
{
    @Override
    public void injectTaskFailure(String traceToken, int stageId, int partitionId, int attemptId, InjectedFailureType injectionType, Optional<ErrorType> errorType)
    {
    }

    @Override
    public Optional<InjectedFailure> getInjectedFailure(String traceToken, int stageId, int partitionId, int attemptId)
    {
        return Optional.empty();
    }

    @Override
    public Duration getRequestTimeout()
    {
        throw new IllegalArgumentException("getRequestTimeout should not be called");
    }
}
