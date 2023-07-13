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

public final class DropCatalog
        extends Statement
{
    private final Identifier catalogName;
    private final boolean exists;
    private final boolean cascade;

    public DropCatalog(Identifier catalogName, boolean exists, boolean cascade)
    {
        this(Optional.empty(), catalogName, exists, cascade);
    }

    public DropCatalog(NodeLocation location, Identifier catalogName, boolean exists, boolean cascade)
    {
        this(Optional.of(location), catalogName, exists, cascade);
    }

    private DropCatalog(Optional<NodeLocation> location, Identifier catalogName, boolean exists, boolean cascade)
    {
        super(location);
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.exists = exists;
        this.cascade = cascade;
    }

    public Identifier getCatalogName()
    {
        return catalogName;
    }

    public boolean isExists()
    {
        return exists;
    }

    public boolean isCascade()
    {
        return cascade;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropCatalog(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        DropCatalog o = (DropCatalog) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                (exists == o.exists) &&
                (cascade == o.cascade);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, exists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("exists", exists)
                .add("cascade", cascade)
                .toString();
    }
}
