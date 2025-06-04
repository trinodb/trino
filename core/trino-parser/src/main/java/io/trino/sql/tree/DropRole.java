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

public class DropRole
        extends Statement
{
    private final Identifier name;
    private final Optional<Identifier> catalog;
    private final boolean exists;

    public DropRole(NodeLocation location, Identifier name, Optional<Identifier> catalog, boolean exists)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.exists = exists;
    }

    public Identifier getName()
    {
        return name;
    }

    public Optional<Identifier> getCatalog()
    {
        return catalog;
    }

    public boolean isExists()
    {
        return exists;
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
        DropRole dropRole = (DropRole) o;
        return Objects.equals(name, dropRole.name) &&
                Objects.equals(catalog, dropRole.catalog) &&
                exists == dropRole.exists;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, catalog, exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("catalog", catalog)
                .add("exists", exists)
                .toString();
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropRole(this, context);
    }
}
