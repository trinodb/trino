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

final class MatchCursors
{
    private MatchCursors() {}

    // Called only while traversing a cursor, so property and custom matching remain lazy.
    public static MatchCursor accept(Pattern<?> pattern, Object object, Captures captures, Object context)
    {
        return switch (pattern) {
            case OrPattern<?> or -> new OrMatchCursor(or, object, captures, context);
            case CustomPattern<?> custom -> custom.accept(object, captures, context);
            case WithPattern<?> with -> matchProperty(with, object, captures, context);
            default -> new SingleMatchCursor(pattern, pattern.previous().orElse(null), object, captures, context);
        };
    }

    @SuppressWarnings("unchecked")
    private static MatchCursor matchProperty(WithPattern<?> pattern, Object object, Captures captures, Object context)
    {
        BiFunction<Object, Object, Optional<?>> property = (BiFunction<Object, Object, Optional<?>>) pattern.getProperty().getFunction();
        Optional<?> value = property.apply(object, context);
        if (value.isEmpty()) {
            return MatchCursor.empty();
        }
        return pattern.getPattern().matchCursor(value.orElseThrow(), captures, context);
    }
}
