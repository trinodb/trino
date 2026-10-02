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
package io.trino.type;

import io.airlift.regulator.TrinoLikePattern;
import io.airlift.slice.Slice;
import io.trino.likematcher.LikeMatcher;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.type.LikeLibrary.REGULATOR;
import static io.trino.type.LikeLibrary.TRINO;
import static java.util.Objects.requireNonNull;

/**
 * LikePattern can be a part of the cache key in projection/filter compiled class caches in ExpressionCompiler.
 * Equality depends on the pattern, escape and library, which determine the matcher.
 */
public class LikePattern
{
    private final String pattern;
    private final Optional<Character> escape;
    private final LikeLibrary library;
    private final Predicate<Slice> matcher;

    public static LikePattern compile(String pattern, Optional<Character> escape)
    {
        return compile(pattern, escape, true);
    }

    public static LikePattern compile(String pattern, Optional<Character> escape, boolean optimize)
    {
        LikeMatcher matcher = LikeMatcher.compile(pattern, escape, optimize);
        return new LikePattern(pattern, escape, TRINO, value -> matcher.match(value.byteArray(), value.byteArrayOffset(), value.length()));
    }

    public static LikePattern compile(String pattern, Optional<Character> escape, LikeLibrary library)
    {
        return switch (library) {
            case TRINO -> compile(pattern, escape);
            case REGULATOR -> {
                Slice patternSlice = utf8Slice(pattern);
                TrinoLikePattern matcher = escape
                        .map(character -> TrinoLikePattern.compile(patternSlice, character))
                        .orElseGet(() -> TrinoLikePattern.compile(patternSlice));
                yield new LikePattern(pattern, escape, REGULATOR, matcher::matches);
            }
        };
    }

    private LikePattern(String pattern, Optional<Character> escape, LikeLibrary library, Predicate<Slice> matcher)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.escape = requireNonNull(escape, "escape is null");
        this.library = requireNonNull(library, "library is null");
        this.matcher = requireNonNull(matcher, "matcher is null");
    }

    public String getPattern()
    {
        return pattern;
    }

    public Optional<Character> getEscape()
    {
        return escape;
    }

    public LikeLibrary getLibrary()
    {
        return library;
    }

    public boolean matches(Slice value)
    {
        return matcher.test(value);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LikePattern that = (LikePattern) o;
        return Objects.equals(pattern, that.pattern) && Objects.equals(escape, that.escape) && library == that.library;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(pattern, escape, library);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("pattern", pattern)
                .add("escape", escape)
                .add("library", library)
                .toString();
    }
}
