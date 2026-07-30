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

/**
 * Decides whether a query should wait for cluster capacity before dispatch.
 *
 * <p>Loaded via Trino's standard {@code Plugin} / {@code ServiceLoader} mechanism.
 * The coordinator binds exactly one {@code AdmissionPolicy} per startup, selected
 * by config key {@code query-manager.admission-policy.name}. The bound instance
 * is retained for the lifetime of the coordinator process; runtime hot-swap is
 * not supported.
 *
 * <p>Implementations MUST be safe for concurrent invocation from multiple threads
 * without external synchronization. The engine may invoke
 * {@link #shouldQueryWait(QueryAdmissionContext)} from any dispatcher thread.
 *
 * <p>Implementations MUST NOT block inside {@link #shouldQueryWait}. The method
 * is expected to return promptly with a {@link WaitDecision}; any waiting is
 * performed by the engine according to the returned decision.
 */
public interface AdmissionPolicy
{
    /**
     * Decide whether the given query should wait, and for how long.
     *
     * <p>Called exactly once per query, at the moment the query enters
     * {@code WAITING_FOR_RESOURCES}. The return value is acted on immediately by
     * the engine; this method MUST NOT block.
     *
     * <p>Returning {@code null} or throwing any {@link Throwable} is treated as a
     * policy failure: the engine fails the query with
     * {@code GENERIC_INSUFFICIENT_RESOURCES}.
     *
     * @param query non-null context describing the query
     * @return non-null decision; never {@code null}
     */
    WaitDecision shouldQueryWait(QueryAdmissionContext query);
}
