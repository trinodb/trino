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

import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class Constant
        extends ConnectorExpression
{
    public static final Constant TRUE = new Constant(true, BooleanType.BOOLEAN);
    public static final Constant FALSE = new Constant(false, BooleanType.BOOLEAN);

    private final Object value;

    /**
     * @param value the value encoded using the native "stack" representation for the given type.
     */
    public Constant(Object value, Type type)
    {
        super(type);
        this.value = value;
    }

    public Object getValue()
    {
        return value;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return emptyList();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, getType());
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

        Constant that = (Constant) o;
        return Objects.equals(this.value, that.value)
                && Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return value + "::" + getType();
    }
}
