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
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class SingleMatchCursor
        implements MatchCursor
{
    private final Pattern<?> pattern;
    private final Pattern<?> previous;
    private final Object object;
    private final Captures inputCaptures;
    private final Object context;
    private boolean consumed;
    private Captures captures;

    public SingleMatchCursor(Captures captures)
    {
        this(null, null, null, requireNonNull(captures, "captures is null"), null);
    }

    public SingleMatchCursor(Pattern<?> pattern, Pattern<?> previous, Object object, Captures captures, Object context)
    {
        this.pattern = pattern;
        this.previous = previous;
        this.object = object;
        this.inputCaptures = captures;
        this.context = context;
    }

    @Override
    public boolean matches()
    {
        captures = null;
        if (consumed) {
            return false;
        }
        consumed = true;
        captures = match(pattern, previous, object, inputCaptures, context);
        return captures != null;
    }

    @Override
    public Captures current()
    {
        checkState(captures != null, "Cursor is not positioned on a match");
        return captures;
    }

    // Evaluate a whole single-match chain without intermediate cursors.
    // The previous pattern has already been matched when this is a branch's continuation.
    @SuppressWarnings("unchecked")
    public static Captures match(Pattern<?> pattern, Pattern<?> previous, Object object, Captures captures, Object context)
    {
        if (pattern == previous) {
            return captures;
        }
        if (pattern.previous().isPresent()) {
            captures = match(pattern.previous().orElseThrow(), previous, object, captures, context);
            if (captures == null) {
                return null;
            }
        }
        return switch (pattern) {
            case TypeOfPattern<?> type -> type.expectedClass().isInstance(object) ? captures : null;
            case EqualsPattern<?> equals -> equals.expectedValue().equals(object) ? captures : null;
            case FilterPattern<?> filter -> ((BiPredicate<Object, Object>) filter.predicate()).test(object, context) ? captures : null;
            case CapturePattern<?> capture -> captures.addAll(Captures.ofNullable((Capture<Object>) capture.capture(), object));
            case WithPattern<?> with -> {
                BiFunction<Object, Object, Optional<?>> property = (BiFunction<Object, Object, Optional<?>>) with.getProperty().getFunction();
                Optional<?> value = property.apply(object, context);
                yield value.isPresent() ? match(with.getPattern(), null, value.orElseThrow(), captures, context) : null;
            }
            case OrPattern<?> _, CustomPattern<?> _ -> throw new IllegalStateException("Pattern can produce multiple matches");
        };
    }
}
