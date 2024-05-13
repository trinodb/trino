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
package io.trino.spi.expression;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

// This class is used to represent expression with dereferences into Array
// Target is the actual reference to the array. elementFieldDereferences are the field dereferences
public class ArrayFieldDereference
        extends ConnectorExpression
{
    private final ConnectorExpression target;
    private final List<ConnectorExpression> elementFieldDereferences;

    public ArrayFieldDereference(Type type, ConnectorExpression target, List<ConnectorExpression> elementFieldDereference)
    {
        super(type);
        checkArgument(type instanceof ArrayType, "wrong input type for ArrayFieldDereference");
        this.target = requireNonNull(target, "target is null");
        this.elementFieldDereferences = ImmutableList.copyOf(requireNonNull(elementFieldDereference, "elementFieldDereference is null"));
    }

    public ConnectorExpression getTarget()
    {
        return target;
    }

    public List<ConnectorExpression> getElementFieldDereferences()
    {
        return elementFieldDereferences;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return singletonList(target);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, elementFieldDereferences, getType());
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

        ArrayFieldDereference that = (ArrayFieldDereference) o;
        return Objects.equals(target, that.target)
                && Objects.equals(elementFieldDereferences, that.elementFieldDereferences)
                && Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return format("(%s).#[%s]", target, elementFieldDereferences.stream()
                .map(item -> "(" + item + ")")
                .collect(joining(" ")));
    }
}
