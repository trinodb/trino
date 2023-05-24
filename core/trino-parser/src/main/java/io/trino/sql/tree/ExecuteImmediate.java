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

public class ExecuteImmediate
        extends Statement
{
    private final StringLiteral statement;
    private final List<Expression> parameters;

    public ExecuteImmediate(NodeLocation location, StringLiteral statement, List<Expression> parameters)
    {
        super(Optional.of(location));
        this.statement = requireNonNull(statement, "statement is null");
        this.parameters = ImmutableList.copyOf(parameters);
    }

    public StringLiteral getStatement()
    {
        return statement;
    }

    public List<Expression> getParameters()
    {
        return parameters;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExecuteImmediate(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.<Node>builder().addAll(parameters).add(statement).build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(statement, parameters);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ExecuteImmediate o = (ExecuteImmediate) obj;
        return Objects.equals(statement, o.statement) &&
                Objects.equals(parameters, o.parameters);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("statement", statement)
                .add("parameters", parameters)
                .toString();
    }
}
