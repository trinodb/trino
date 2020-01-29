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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class SetSchemaAuthorization
        extends Statement
{
    private final QualifiedName source;
    private final PrincipalSpecification principal;

    public SetSchemaAuthorization(QualifiedName source, PrincipalSpecification principal)
    {
        this(Optional.empty(), source, principal);
    }

    public SetSchemaAuthorization(NodeLocation location, QualifiedName source, PrincipalSpecification principal)
    {
        this(Optional.of(location), source, principal);
    }

    private SetSchemaAuthorization(Optional<NodeLocation> location, QualifiedName source, PrincipalSpecification principal)
    {
        super(location);
        this.source = requireNonNull(source, "source is null");
        this.principal = requireNonNull(principal, "principal is null");
    }

    public QualifiedName getSource()
    {
        return source;
    }

    public PrincipalSpecification getPrincipal()
    {
        return principal;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetSchemaAuthorization(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, principal);
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
        SetSchemaAuthorization o = (SetSchemaAuthorization) obj;
        return Objects.equals(source, o.source) &&
                Objects.equals(principal, o.principal);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", source)
                .add("principal", principal)
                .toString();
    }
}
