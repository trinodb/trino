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

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static io.trino.sql.ir.IrUtils.validateType;
import static java.util.Objects.requireNonNull;

public record Array(Type elementType, List<Expression> elements)
        implements Expression
{
    public Array
    {
        requireNonNull(elementType, "type is null");
        elements = ImmutableList.copyOf(elements);

        for (Expression item : elements) {
            validateType(elementType, item);
        }
    }

    @Override
    public Type type()
    {
        return new ArrayType(elementType);
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitArray(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        return elements;
    }

    @Override
    public String toString()
    {
        return "[" +
                elements.stream()
                        .map(Expression::toString)
                        .collect(Collectors.joining(", ")) +
                "]";
    }
}
