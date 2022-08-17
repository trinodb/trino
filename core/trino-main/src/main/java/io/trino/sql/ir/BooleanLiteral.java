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

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

@Immutable
public class BooleanLiteral
        extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = BooleanLiteral.of("true");
    public static final BooleanLiteral FALSE_LITERAL = BooleanLiteral.of("false");

    private final boolean value;

    public static BooleanLiteral of(String value)
    {
        requireNonNull(value, "value is null");
        checkArgument(value.toLowerCase(ENGLISH).equals("true") || value.toLowerCase(ENGLISH).equals("false"));

        boolean booleanValue = value.toLowerCase(ENGLISH).equals("true");

        return new BooleanLiteral(booleanValue);
    }

    @JsonCreator
    public BooleanLiteral(
            @JsonProperty("value") boolean value)
    {
        this.value = value;
    }

    @JsonProperty
    public boolean getValue()
    {
        return value;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitBooleanLiteral(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BooleanLiteral other = (BooleanLiteral) obj;
        return Objects.equals(this.value, other.value);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return value == ((BooleanLiteral) other).value;
    }
}
