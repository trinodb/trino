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

public class DropFunction
        extends Statement
{
    private final QualifiedName name;
    private final List<ParameterDeclaration> parameters;
    private final boolean exists;

    public DropFunction(NodeLocation location, QualifiedName name, List<ParameterDeclaration> parameters, boolean exists)
    {
        super(Optional.of(location));
        this.name = requireNonNull(name, "name is null");
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.exists = exists;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<ParameterDeclaration> getParameters()
    {
        return parameters;
    }

    public boolean isExists()
    {
        return exists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropFunction(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, exists);
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
        DropFunction o = (DropFunction) obj;
        return Objects.equals(name, o.name)
                && (exists == o.exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("exists", exists)
                .toString();
    }
}
