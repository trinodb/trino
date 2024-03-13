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
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;

import java.util.Arrays;
import java.util.List;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class BinaryLiteral
        extends Literal
{
    // the grammar could possibly include whitespace in the value it passes to us
    private static final CharMatcher WHITESPACE_MATCHER = CharMatcher.whitespace();

    private final byte[] value;

    @JsonCreator
    public BinaryLiteral(byte[] value)
    {
        this.value = value.clone();
    }

    public BinaryLiteral(String value)
    {
        requireNonNull(value, "value is null");
        String hexString = WHITESPACE_MATCHER.removeFrom(value).toUpperCase(ENGLISH);
        this.value = BaseEncoding.base16().decode(hexString);
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
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of();
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
    public String toString()
    {
        return "Binary[%s]".formatted(toHexString());
    }
}
