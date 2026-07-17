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
package io.trino.json.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slices;
import io.trino.operator.scalar.JoniRegexpCasts;
import io.trino.type.JoniRegexp;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/// `like_regex` JSON path predicate. The pattern is pre-translated from XQuery syntax to Java
/// regex syntax (with inline flags) at IR construction time, and compiled to a [JoniRegexp]
/// eagerly in the constructor. The runtime visitor then just invokes the regex engine — no
/// per-row resolution or compilation.
public final class IrLikeRegexPredicate
        implements IrPredicate
{
    private final IrPathNode path;
    private final String pattern;
    private final JoniRegexp regex;

    @JsonCreator
    public IrLikeRegexPredicate(@JsonProperty("path") IrPathNode path, @JsonProperty("pattern") String pattern)
    {
        this.path = requireNonNull(path, "path is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.regex = JoniRegexpCasts.joniRegexp(Slices.utf8Slice(pattern));
    }

    @JsonProperty
    public IrPathNode path()
    {
        return path;
    }

    @JsonProperty
    public String pattern()
    {
        return pattern;
    }

    public JoniRegexp regex()
    {
        return regex;
    }

    @Override
    public <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrLikeRegexPredicate(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IrLikeRegexPredicate other)) {
            return false;
        }
        return path.equals(other.path) && pattern.equals(other.pattern);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, pattern);
    }
}
