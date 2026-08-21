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

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

/**
 * Decision returned by {@link AdmissionPolicy#shouldQueryWait}.
 *
 * <p>Two terminal cases:
 * <ul>
 *   <li>{@link ProceedNow} — skip the gate; start execution immediately.</li>
 *   <li>{@link Wait} — gate the query until its release condition is satisfied
 *       or {@code maxWait} elapses, after which the engine fails the query with
 *       {@code GENERIC_INSUFFICIENT_RESOURCES}.</li>
 * </ul>
 *
 * <p>A {@link Wait} carries an optional {@code releaseCondition} future. When
 * present, the engine admits the query once that future completes normally
 * (and fails it if the future completes exceptionally or {@code maxWait}
 * elapses). When empty, the engine applies its built-in cluster-capacity gate
 * (wait for the minimum required workers) — this is the behavior used by the
 * default {@code min-workers} policy. Custom policies that need to gate on a
 * different signal (memory headroom, a concurrency slot, an external quota,
 * ...) complete their own future and return it via {@link Wait#forCondition}.
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
     * Gate the query until its release condition is satisfied or {@code maxWait}
     * elapses.
     *
     * <p>Prefer the {@link #forClusterCapacity} and {@link #forCondition}
     * factories over the canonical constructor.
     *
     * @param releaseCondition when present, the future the engine awaits before
     *         admitting the query; when empty, the engine uses its built-in
     *         cluster-capacity gate. Never {@code null}.
     * @param maxWait non-null, non-negative upper bound on the wait. For the
     *         built-in cluster-capacity gate the engine may apply its own
     *         configured timeout instead.
     * @param reason non-null reason string of length {@code [1, 256]}
     */
    record Wait(Optional<CompletableFuture<Void>> releaseCondition, Duration maxWait, String reason)
            implements WaitDecision
    {
        public Wait
        {
            requireNonNull(releaseCondition, "releaseCondition is null");
            requireNonNull(maxWait, "maxWait is null");
            if (maxWait.isNegative()) {
                throw new IllegalArgumentException("maxWait must be non-negative");
            }
            validateReason(reason);
        }

        /**
         * Gate on the engine's built-in cluster-capacity condition (wait for the
         * minimum required workers). Used by the default {@code min-workers}
         * policy; available to any policy that wants today's behavior.
         */
        public static Wait forClusterCapacity(Duration maxWait, String reason)
        {
            return new Wait(Optional.empty(), maxWait, reason);
        }

        /**
         * Gate on a policy-supplied condition. The engine admits the query when
         * {@code releaseCondition} completes normally, and fails it if the future
         * completes exceptionally or {@code maxWait} elapses first. The engine
         * cancels the future if the query is cancelled or otherwise finishes.
         */
        public static Wait forCondition(CompletableFuture<Void> releaseCondition, Duration maxWait, String reason)
        {
            return new Wait(Optional.of(requireNonNull(releaseCondition, "releaseCondition is null")), maxWait, reason);
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
