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

import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

// This class is used to represent expression with dereference into Array
// Target is the actual symbol reference to the array. elementFieldDereferences are the subscripts
public class ArrayFieldDereference
        extends ConnectorExpression
{
    private final ConnectorExpression target;
    private final List<ConnectorExpression> elementFieldDereferences;

    public ArrayFieldDereference(Type type, ConnectorExpression target, List<ConnectorExpression> elementFieldDereference)
    {
        super(type);
        this.target = requireNonNull(target, "target is null");
        this.elementFieldDereferences = requireNonNull(elementFieldDereference, "elementFieldDereference is null");
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
        return format("(%s).#%s", target, elementFieldDereferences);
    }
}
