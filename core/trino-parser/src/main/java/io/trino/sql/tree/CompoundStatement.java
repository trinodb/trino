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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class CompoundStatement
        extends ControlStatement
{
    private final List<VariableDeclaration> variableDeclarations;
    private final List<ControlStatement> statements;

    public CompoundStatement(
            NodeLocation location,
            List<VariableDeclaration> variableDeclarations,
            List<ControlStatement> statements)
    {
        super(location);
        this.variableDeclarations = requireNonNull(variableDeclarations, "variableDeclarations is null");
        this.statements = requireNonNull(statements, "statements is null");
    }

    public List<ControlStatement> getStatements()
    {
        return statements;
    }

    public List<VariableDeclaration> getVariableDeclarations()
    {
        return variableDeclarations;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCompoundStatement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(statements)
                .addAll(variableDeclarations)
                .build();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof CompoundStatement other) &&
                Objects.equals(variableDeclarations, other.variableDeclarations) &&
                Objects.equals(statements, other.statements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(variableDeclarations, statements);
    }

    @Override

    public String toString()
    {
        return toStringHelper(this)
                .add("variableDeclarations", variableDeclarations)
                .add("statements", statements)
                .toString();
    }
}
