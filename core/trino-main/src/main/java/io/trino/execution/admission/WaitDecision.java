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
package io.trino.execution.admission;

import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

/**
 * The decision a {@link ResourceAwareAdmissionController} hands back to the engine for a single
 * query entering the {@code WAITING_FOR_RESOURCES} state.
 * <p>
 * Two terminal cases:
 * <ul>
 *     <li>{@link ProceedNow} — the cluster currently has enough capacity; admit the query now.</li>
 *     <li>{@link Wait} — hold the query until capacity frees up or {@code maxWait} elapses.</li>
 * </ul>
 * The shape intentionally mirrors a pluggable admission-policy SPI, so the decision logic can be
 * factored into a separate policy interface later without reshaping callers.
 */
public sealed interface WaitDecision
        permits WaitDecision.ProceedNow, WaitDecision.Wait
{
    /**
     * Human-readable reason for observability (logs, events, UI). Must be 1..256 chars, non-null.
     */
    String reason();

    record ProceedNow(String reason)
            implements WaitDecision
    {
        public ProceedNow
        {
            validateReason(reason);
        }
    }

    record Wait(Duration maxWait, String reason)
            implements WaitDecision
    {
        public Wait
        {
            requireNonNull(maxWait, "maxWait is null");
            if (maxWait.toMillis() < 0) {
                throw new IllegalArgumentException("maxWait must not be negative");
            }
            validateReason(reason);
        }
    }

    private static void validateReason(String reason)
    {
        requireNonNull(reason, "reason is null");
        if (reason.isEmpty() || reason.length() > 256) {
            throw new IllegalArgumentException("reason must be between 1 and 256 characters");
        }
    }
}
