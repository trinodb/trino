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
package io.prestosql.spi.expression;

import io.prestosql.spi.type.Type;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FieldDereference
        extends ConnectorExpression
{
    private final ConnectorExpression target;
    private final int field;

    public FieldDereference(Type type, ConnectorExpression target, int field)
    {
        super(type);
        this.target = requireNonNull(target, "target is null");
        this.field = field;
    }

    public ConnectorExpression getTarget()
    {
        return target;
    }

    public int getField()
    {
        return field;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, field, getType());
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

        FieldDereference that = (FieldDereference) o;
        return Objects.equals(target, that.target)
                && Objects.equals(field, that.field)
                && Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return format("(%s).#%s", target, field);
    }
}
