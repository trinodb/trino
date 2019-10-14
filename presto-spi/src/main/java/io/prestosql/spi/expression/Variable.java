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

import static java.util.Objects.requireNonNull;

public class Variable
        extends ConnectorExpression
{
    private final String name;

    public Variable(String name, Type type)
    {
        super(type);
        this.name = requireNonNull(name, "name is null");

        if (name.isEmpty()) {
            throw new IllegalArgumentException("name is empty");
        }
    }

    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, getType());
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

        Variable that = (Variable) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public String toString()
    {
        return name + "::" + getType();
    }
}
