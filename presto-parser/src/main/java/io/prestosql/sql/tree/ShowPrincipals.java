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

public class ShowPrincipals
        extends Statement
{
    private final Identifier role;
    private final Optional<Identifier> catalog;

    public ShowPrincipals(NodeLocation location, Identifier role, Optional<Identifier> catalog)
    {
        this(Optional.of(location), role, catalog);
    }

    public ShowPrincipals(Optional<NodeLocation> location, Identifier role, Optional<Identifier> catalog)
    {
        super(location);
        this.role = role;
        this.catalog = catalog;
    }

    public Identifier getRole()
    {
        return role;
    }

    public Optional<Identifier> getCatalog()
    {
        return catalog;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowPrincipals(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(role);
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
        ShowPrincipals o = (ShowPrincipals) obj;
        return Objects.equals(role, o.role);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("role", role)
                .toString();
    }
}
