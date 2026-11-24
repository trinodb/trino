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

import static com.google.common.base.Preconditions.checkState;

final class OrMatchCursor
        implements MatchCursor
{
    private final OrPattern<?> pattern;
    private final Object object;
    private final Captures inputCaptures;
    private final Object context;
    private int next;
    private MatchCursor current;
    private Captures captures;

    public OrMatchCursor(OrPattern<?> pattern, Object object, Captures captures, Object context)
    {
        this.pattern = pattern;
        this.object = object;
        this.inputCaptures = captures;
        this.context = context;
    }

    @Override
    public boolean matches()
    {
        captures = null;
        while (true) {
            if (current != null) {
                if (current.matches()) {
                    captures = current.current();
                    return true;
                }
                current = null;
            }
            if (next == pattern.getPatterns().size()) {
                return false;
            }
            current = MatchCursors.accept(pattern.getPatterns().get(next++), object, inputCaptures, context);
        }
    }

    @Override
    public Captures current()
    {
        checkState(captures != null, "Cursor is not positioned on a match");
        return captures;
    }
}
