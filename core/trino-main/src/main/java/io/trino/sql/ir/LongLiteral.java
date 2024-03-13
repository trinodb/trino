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
import com.google.common.collect.ImmutableList;
import io.trino.sql.parser.ParsingException;

import java.util.List;

import static java.util.Objects.requireNonNull;

public final class LongLiteral
        extends Literal
{
    private final String value;
    private final long parsedValue;

    @JsonCreator
    public LongLiteral(long parsedValue)
    {
        this.parsedValue = parsedValue;
        this.value = Long.toString(parsedValue);
    }

    @Deprecated
    public LongLiteral(String value)
    {
        requireNonNull(value, "value is null");
        try {
            this.value = value;
            this.parsedValue = parse(value);
        }
        catch (NumberFormatException e) {
            throw new ParsingException("Invalid numeric literal: " + value);
        }
    }

    public String getValue()
    {
        return value;
    }

    @JsonProperty
    public long getParsedValue()
    {
        return parsedValue;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLongLiteral(this, context);
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

        LongLiteral that = (LongLiteral) o;

        if (parsedValue != that.parsedValue) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return (int) (parsedValue ^ (parsedValue >>> 32));
    }

    private static long parse(String value)
    {
        value = value.replace("_", "");

        if (value.startsWith("0x") || value.startsWith("0X")) {
            return Long.parseLong(value.substring(2), 16);
        }
        else if (value.startsWith("0b") || value.startsWith("0B")) {
            return Long.parseLong(value.substring(2), 2);
        }
        else if (value.startsWith("0o") || value.startsWith("0O")) {
            return Long.parseLong(value.substring(2), 8);
        }
        else {
            return Long.parseLong(value);
        }
    }

    @Override
    public String toString()
    {
        return value;
    }
}
