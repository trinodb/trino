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

final class ChainedMatchCursor
        implements MatchCursor
{
    private final Pattern<?> pattern;
    private final Pattern<?> branch;
    private final Object object;
    private final Object context;
    // Matches of the steps before the branch
    private final MatchCursor input;
    private MatchCursor alternatives;
    private Captures captures;

    public ChainedMatchCursor(Pattern<?> pattern, Pattern<?> branch, Object object, Captures captures, Object context)
    {
        this.pattern = pattern;
        this.branch = branch;
        this.object = object;
        this.context = context;
        // Creating a cursor does not evaluate anything, so laziness is preserved
        if (branch.previousBranch() != null) {
            this.input = new ChainedMatchCursor(branch.previous().orElseThrow(), branch.previousBranch(), object, captures, context);
        }
        else {
            this.input = new SingleMatchCursor(branch.previous().orElse(null), null, object, captures, context);
        }
    }

    // The last step that can produce multiple matches; null means the entire chain is single-match.
    public static Pattern<?> findBranch(Pattern<?> pattern)
    {
        return switch (pattern) {
            case OrPattern<?> _, CustomPattern<?> _ -> pattern;
            case WithPattern<?> with -> findBranch(with.getPattern()) == null ? pattern.previousBranch() : pattern;
            case TypeOfPattern<?> _, EqualsPattern<?> _, FilterPattern<?> _, CapturePattern<?> _ -> pattern.previousBranch();
        };
    }

    @Override
    public boolean matches()
    {
        captures = null;
        while (true) {
            if (alternatives != null) {
                while (alternatives.matches()) {
                    captures = SingleMatchCursor.match(pattern, branch, object, alternatives.current(), context);
                    if (captures != null) {
                        return true;
                    }
                }
                alternatives = null;
            }
            if (!input.matches()) {
                return false;
            }
            alternatives = MatchCursors.accept(branch, object, input.current(), context);
        }
    }

    @Override
    public Captures current()
    {
        checkState(captures != null, "Cursor is not positioned on a match");
        return captures;
    }
}
