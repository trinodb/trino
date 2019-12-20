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
package io.prestosql.spi.connector;

import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ProjectionApplicationResult<T>
{
    private final T handle;
    private final List<ConnectorExpression> projections;
    private final List<Assignment> assignments;

    public ProjectionApplicationResult(T handle, List<ConnectorExpression> projections, List<Assignment> assignments)
    {
        requireNonNull(projections, "projections is null");
        requireNonNull(assignments, "assignments is null");

        this.handle = requireNonNull(handle, "handle is null");

        this.projections = unmodifiableList(new ArrayList<>(projections));
        this.assignments = unmodifiableList(new ArrayList<>(assignments));
    }

    public T getHandle()
    {
        return handle;
    }

    public List<ConnectorExpression> getProjections()
    {
        return projections;
    }

    public List<Assignment> getAssignments()
    {
        return assignments;
    }

    public static class Assignment
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
    }
}
