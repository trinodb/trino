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
import com.google.common.io.BaseEncoding;

import javax.annotation.concurrent.Immutable;

import java.util.Arrays;
import java.util.function.Function;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@Immutable
public class BinaryLiteral
        extends Literal
{
    // the grammar could possibly include whitespace in the value it passes to us
    private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();
    private static final CharMatcher HEX_DIGIT_MATCHER = CharMatcher.inRange('A', 'F')
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();

    private final byte[] value;

    private static final Function<String, String> hexChecker = hex -> {
        if (!HEX_DIGIT_MATCHER.matchesAllOf(hex)) {
            throw new IllegalArgumentException();
        }
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException();
        }
        return hex;
    };

    public BinaryLiteral(String value)
    {
        this(BaseEncoding.base16().decode(hexChecker.apply(WHITESPACE_MATCHER.removeFrom(requireNonNull(value, "value is null")).toUpperCase(ENGLISH))));
    }

    @JsonCreator
    public BinaryLiteral(
            @JsonProperty("value") byte[] value)
    {
        this.value = value;
    }

    /**
     * Return the valued as a hex-formatted string with upper-case characters
     */
    public String toHexString()
    {
        return BaseEncoding.base16().encode(value);
    }

    @JsonProperty
    public byte[] getValue()
    {
        return value.clone();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitBinaryLiteral(this, context);
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

        BinaryLiteral that = (BinaryLiteral) o;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(value);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return Arrays.equals(value, ((BinaryLiteral) other).value);
    }
}
