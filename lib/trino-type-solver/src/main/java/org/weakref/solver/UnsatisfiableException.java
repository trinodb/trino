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
package org.weakref.solver;

/**
 * Thrown by the solver when a constraint is proven impossible — e.g. an upper and lower
 * bound conflict, or a coercion is known to be absent for ground types.
 * <p>
 * The top-level {@link Solver#solveOutcome} catches this and translates it to a
 * {@link Solver.Unsatisfied} outcome so callers can handle failure without exception
 * plumbing. The direct {@link Solver#solve} entrypoint lets it propagate.
 */
public final class UnsatisfiableException
        extends RuntimeException
{
    public UnsatisfiableException(String message)
    {
        // Used for control flow inside the solver (an unsatisfiable branch), not to report a bug, and
        // thrown often on the resolution hot path — so skip the cost of capturing a stack trace.
        super(message, null, false, false);
    }
}
