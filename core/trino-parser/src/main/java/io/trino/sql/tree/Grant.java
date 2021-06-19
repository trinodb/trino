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

public class Grant
        extends Statement
{
    private final Optional<List<String>> privileges; // missing means ALL PRIVILEGES
    private final Optional<GrantOnType> type;
    private final QualifiedName name;
    private final PrincipalSpecification grantee;
    private final boolean grantOption;

    public Grant(Optional<List<String>> privileges, Optional<GrantOnType> type, QualifiedName name, PrincipalSpecification grantee, boolean grantOption)
    {
        this(Optional.empty(), privileges, type, name, grantee, grantOption);
    }

    public Grant(NodeLocation location, Optional<List<String>> privileges, Optional<GrantOnType> type, QualifiedName name, PrincipalSpecification grantee, boolean grantOption)
    {
        this(Optional.of(location), privileges, type, name, grantee, grantOption);
    }

    private Grant(Optional<NodeLocation> location, Optional<List<String>> privileges, Optional<GrantOnType> type, QualifiedName name, PrincipalSpecification grantee, boolean grantOption)
    {
        super(location);
        requireNonNull(privileges, "privileges is null");
        this.privileges = privileges.map(ImmutableList::copyOf);
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null");
        this.grantee = requireNonNull(grantee, "grantee is null");
        this.grantOption = grantOption;
    }

    public Optional<List<String>> getPrivileges()
    {
        return privileges;
    }

    public Optional<GrantOnType> getType()
    {
        return type;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public PrincipalSpecification getGrantee()
    {
        return grantee;
    }

    public boolean isWithGrantOption()
    {
        return grantOption;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitGrant(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(privileges, type, name, grantee, grantOption);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Grant o = (Grant) obj;
        return Objects.equals(privileges, o.privileges) &&
                Objects.equals(type, o.type) &&
                Objects.equals(name, o.name) &&
                Objects.equals(grantee, o.grantee) &&
                Objects.equals(grantOption, o.grantOption);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("privileges", privileges)
                .add("type", type)
                .add("name", name)
                .add("grantee", grantee)
                .add("grantOption", grantOption)
                .toString();
    }
}
