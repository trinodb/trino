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

public class CreateCatalogLike
        extends Statement
{
    private final Identifier source;
    private final Identifier target;
    private final boolean notExists;
    private final List<Property> properties;

    public CreateCatalogLike(
            Identifier source,
            Identifier target,
            boolean notExists,
            List<Property> properties)
    {
        this(Optional.empty(), source, target, notExists, properties);
    }

    public CreateCatalogLike(
            NodeLocation location,
            Identifier source,
            Identifier target,
            boolean notExists,
            List<Property> properties)
    {
        this(Optional.of(location), source, target, notExists, properties);
    }

    private CreateCatalogLike(
            Optional<NodeLocation> location,
            Identifier source,
            Identifier target,
            boolean notExists,
            List<Property> properties)
    {
        super(location);
        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.notExists = notExists;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
    }

    public Identifier getSource()
    {
        return source;
    }

    public Identifier getTarget()
    {
        return target;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateCatalogLike(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.copyOf(properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, target, properties, properties);
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
        CreateCatalogLike o = (CreateCatalogLike) obj;
        return Objects.equals(source, o.source) &&
                Objects.equals(target, o.target) &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", source)
                .add("target", target)
                .add("properties", properties)
                .toString();
    }
}
