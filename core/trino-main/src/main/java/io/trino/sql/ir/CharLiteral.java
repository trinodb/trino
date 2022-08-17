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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.CharMatcher;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class CharLiteral
        extends Literal
{
    private final String value;
    private final int length;

    public static CharLiteral of(String value)
    {
        requireNonNull(value, "value is null");
        return new CharLiteral(CharMatcher.is(' ').trimTrailingFrom(value));
    }

    @JsonCreator
    public CharLiteral(
            @JsonProperty("value") String value)
    {
        this.length = value.length();
        this.value = value;
    }

    @JsonProperty
    public String getValue()
    {
        return value;
    }

    public int length()
    {
        return length;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitCharLiteral(this, context);
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
        CharLiteral that = (CharLiteral) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return Objects.equals(value, ((CharLiteral) other).value);
    }
}
