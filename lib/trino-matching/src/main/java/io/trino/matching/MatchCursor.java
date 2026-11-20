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
package io.trino.matching;

import java.util.Optional;

/**
 * A single-use, sequential traversal of a pattern's matches. Creating a cursor
 * does not evaluate predicates, properties, or custom patterns. Each call to
 * {@link #matches()} evaluates only enough of the pattern to find the next match.
 * Cursors are not thread-safe.
 */
public sealed interface MatchCursor
        permits ChainedMatchCursor,
                EmptyMatchCursor,
                OrMatchCursor,
                SingleMatchCursor
{
    /**
     * Advances this cursor to the next match.
     * Returns false after exhaustion.
     */
    boolean matches();

    /**
     * Returns the current match's immutable captures, which may be retained after
     * advancing the cursor.
     *
     * @throws IllegalStateException if the cursor is not positioned on a match
     */
    Captures current();

    /**
     * Returns the first remaining match, without evaluating later matches.
     */
    default Optional<Captures> findFirst()
    {
        return matches() ? Optional.of(current()) : Optional.empty();
    }

    static MatchCursor empty()
    {
        return EmptyMatchCursor.INSTANCE;
    }

    static MatchCursor single(Captures captures)
    {
        return new SingleMatchCursor(captures);
    }
}
