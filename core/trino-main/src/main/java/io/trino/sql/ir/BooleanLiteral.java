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

import java.util.List;
import java.util.Objects;

public final class BooleanLiteral
        extends Literal
{
    public static final BooleanLiteral TRUE_LITERAL = new BooleanLiteral(true);
    public static final BooleanLiteral FALSE_LITERAL = new BooleanLiteral(false);

    private final boolean value;

    @JsonCreator
    public BooleanLiteral(boolean value)
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
    public List<? extends Expression> getChildren()
    {
        return ImmutableList.of();
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
    public String toString()
    {
        return Boolean.toString(value);
    }
}
