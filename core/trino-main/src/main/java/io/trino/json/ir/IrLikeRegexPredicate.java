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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.trino.json.regex.XQuerySqlRegex;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record IrLikeRegexPredicate(IrPathNode value, @JsonIgnore XQuerySqlRegex regex)
        implements IrPredicate
{
    public IrLikeRegexPredicate
    {
        requireNonNull(value, "value is null");
        requireNonNull(regex, "regex is null");
    }

    @JsonCreator
    @DoNotCall
    public static IrLikeRegexPredicate fromJson(
            @JsonProperty("value") IrPathNode value,
            @JsonProperty("pattern") String pattern,
            @JsonProperty("flags") Optional<String> flags)
    {
        XQuerySqlRegex regex = XQuerySqlRegex.compile(pattern, flags);
        return new IrLikeRegexPredicate(value, regex);
    }

    @Override
    public <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrLikeRegexPredicate(this, context);
    }

    @JsonProperty
    public IrPathNode getValue()
    {
        return value;
    }

    @JsonProperty
    public String getPattern()
    {
        return regex.getPattern();
    }

    @JsonProperty
    public Optional<String> getFlags()
    {
        return regex.getFlags();
    }
}
