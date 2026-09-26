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
package io.trino.jsonpath.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.trino.jsonpath.JsonPathRegex;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/// A JSON path `like_regex` predicate with a translated pattern and the configured regex type.
/// Compiles once on construction, including after deserialization on a worker.
public final class IrLikeRegexPredicate
        implements IrPredicate
{
    private final IrPathNode path;
    private final String pattern;
    private final Type regexType;
    private final Predicate<Slice> regex;

    @JsonCreator
    public IrLikeRegexPredicate(
            @JsonProperty("path") IrPathNode path,
            @JsonProperty("pattern") String pattern,
            @JsonProperty("regexType") Type regexType)
    {
        this.path = requireNonNull(path, "path is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.regexType = requireNonNull(regexType, "regexType is null");
        this.regex = JsonPathRegex.compile(regexType, pattern);
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

    @JsonProperty
    public Type regexType()
    {
        return regexType;
    }

    public boolean matches(Slice source)
    {
        return regex.test(source);
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
        return path.equals(other.path) && pattern.equals(other.pattern) && regexType.equals(other.regexType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(path, pattern, regexType);
    }
}
