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
package io.trino.sql.tree;

import io.trino.sql.parser.ParsingException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LongLiteral
        extends Literal
{
    private final String value;
    private final long parsedValue;

    public LongLiteral(String value)
    {
        this(Optional.empty(), value);
    }

    public LongLiteral(NodeLocation location, String value)
    {
        this(Optional.of(location), value);
    }

    private LongLiteral(Optional<NodeLocation> location, String value)
    {
        super(location);
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

    public long getParsedValue()
    {
        return parsedValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLongLiteral(this, context);
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

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return parsedValue == ((LongLiteral) other).parsedValue;
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
}
