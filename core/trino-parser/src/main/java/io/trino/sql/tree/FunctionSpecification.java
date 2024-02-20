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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class FunctionSpecification
        extends Node
{
    private final QualifiedName name;
    private final List<ParameterDeclaration> parameters;
    private final ReturnsClause returnsClause;
    private final List<RoutineCharacteristic> routineCharacteristics;
    private final ControlStatement statement;

    public FunctionSpecification(
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnsClause returnsClause,
            List<RoutineCharacteristic> routineCharacteristics,
            ControlStatement statement)
    {
        this(Optional.empty(), name, parameters, returnsClause, routineCharacteristics, statement);
    }

    public FunctionSpecification(
            NodeLocation location,
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnsClause returnsClause,
            List<RoutineCharacteristic> routineCharacteristics,
            ControlStatement statement)
    {
        this(Optional.of(location), name, parameters, returnsClause, routineCharacteristics, statement);
    }

    private FunctionSpecification(
            Optional<NodeLocation> location,
            QualifiedName name,
            List<ParameterDeclaration> parameters,
            ReturnsClause returnsClause,
            List<RoutineCharacteristic> routineCharacteristics,
            ControlStatement statement)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.returnsClause = requireNonNull(returnsClause, "returnClause is null");
        this.routineCharacteristics = ImmutableList.copyOf(requireNonNull(routineCharacteristics, "routineCharacteristics is null"));
        this.statement = requireNonNull(statement, "statement is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<ParameterDeclaration> getParameters()
    {
        return parameters;
    }

    public ReturnsClause getReturnsClause()
    {
        return returnsClause;
    }

    public List<RoutineCharacteristic> getRoutineCharacteristics()
    {
        return routineCharacteristics;
    }

    public ControlStatement getStatement()
    {
        return statement;
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(parameters)
                .add(returnsClause)
                .addAll(routineCharacteristics)
                .add(statement)
                .build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFunctionSpecification(this, context);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof FunctionSpecification other) &&
                Objects.equals(name, other.name) &&
                Objects.equals(parameters, other.parameters) &&
                Objects.equals(returnsClause, other.returnsClause) &&
                Objects.equals(routineCharacteristics, other.routineCharacteristics) &&
                Objects.equals(statement, other.statement);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, parameters, returnsClause, routineCharacteristics, statement);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("parameters", parameters)
                .add("returnsClause", returnsClause)
                .add("routineCharacteristics", routineCharacteristics)
                .add("statement", statement)
                .toString();
    }
}
