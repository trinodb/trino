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

import static java.util.Objects.requireNonNull;

public final class PathElement
        extends Node
{
    private final Optional<Identifier> catalog;
    private final Identifier schema;

    public PathElement(NodeLocation location, Optional<Identifier> catalog, Identifier schema)
    {
        super(location);
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public Optional<Identifier> getCatalog()
    {
        return catalog;
    }

    public Identifier getSchema()
    {
        return schema;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPathElement(this, context);
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
        PathElement o = (PathElement) obj;
        return Objects.equals(schema, o.schema) &&
                Objects.equals(catalog, o.catalog);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema);
    }

    @Override
    public String toString()
    {
        if (catalog.isPresent()) {
            return catalog.get() + "." + schema;
        }
        return schema.toString();
    }
}
