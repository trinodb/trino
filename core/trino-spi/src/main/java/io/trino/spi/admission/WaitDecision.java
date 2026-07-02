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
package io.trino.spi.admission;

import io.airlift.units.Duration;

import static java.util.Objects.requireNonNull;

/**
 * Decision returned by {@link AdmissionPolicy#shouldQueryWait}.
 *
 * <p>Two terminal cases:
 * <ul>
 *   <li>{@link ProceedNow} — skip the gate; start execution immediately.</li>
 *   <li>{@link Wait} — gate the query under the policy's release condition
 *       until either the condition is met or {@code maxWait} elapses, after
 *       which the engine fails the query with
 *       {@code GENERIC_INSUFFICIENT_RESOURCES}.</li>
 * </ul>
 *
 * <p>Each variant exposes a non-null {@link #reason()} string of length
 * {@code [1, 256]} intended for observability (logs, events, UI).
 */
public sealed interface WaitDecision
        permits WaitDecision.ProceedNow, WaitDecision.Wait
{
    /**
     * Human-readable reason. Non-null; length in {@code [1, 256]}.
     */
    String reason();

    /**
     * Skip the admission gate entirely.
     */
    record ProceedNow(String reason)
            implements WaitDecision
    {
        public ProceedNow
        {
            validateReason(reason);
        }
    }

    /**
     * Wait under the policy's release condition for at most {@code maxWait}.
     *
     * @param maxWait non-null, non-negative wait duration
     * @param reason non-null reason string of length {@code [1, 256]}
     */
    record Wait(Duration maxWait, String reason)
            implements WaitDecision
    {
        public Wait
        {
            requireNonNull(maxWait, "maxWait is null");
            if (maxWait.toMillis() < 0) {
                throw new IllegalArgumentException("maxWait must be non-negative");
            }
            validateReason(reason);
        }
    }

    private static void validateReason(String reason)
    {
        requireNonNull(reason, "reason is null");
        int length = reason.length();
        if (length < 1 || length > 256) {
            throw new IllegalArgumentException("reason length must be in [1, 256], got " + length);
        }
    }
}
