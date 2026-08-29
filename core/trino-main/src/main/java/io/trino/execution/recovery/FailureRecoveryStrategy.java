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
package io.trino.execution.recovery;

import io.trino.spi.ErrorCode;

import java.util.function.Consumer;

/**
 * Owns a scheduler's reaction to a distributed-stages failure
 */
public interface FailureRecoveryStrategy
{
    /**
     * @param failQuery fails the query; invoked by a strategy to reject a failure as not worth recovering from
     * @return true if handled (recovery in flight, or {@code failQuery} invoked) and the caller must do nothing further;
     *         false if declined, and the caller must try the next strategy or invoke {@code failQuery} itself
     */
    boolean handleFailure(Throwable failure, ErrorCode errorCode, Consumer<Throwable> failQuery);
}
