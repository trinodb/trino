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
package io.trino.spi.connector;

import io.trino.spi.type.Type;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Assignment
{
    private final String variable;
    private final ColumnHandle column;
    private final Type type;

    public Assignment(String variable, ColumnHandle column, Type type)
    {
        this.variable = requireNonNull(variable, "variable is null");
        this.column = requireNonNull(column, "column is null");
        this.type = requireNonNull(type, "type is null");
    }

    public String getVariable()
    {
        return variable;
    }

    public ColumnHandle getColumn()
    {
        return column;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return format("%s: %s (%s)", variable, column, type);
    }
}
