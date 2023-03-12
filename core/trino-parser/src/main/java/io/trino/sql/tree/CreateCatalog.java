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

public class CreateCatalog
        extends Statement
{
    private final Identifier catalogName;
    private final boolean notExists;
    private final Identifier connectorName;
    private final List<Property> properties;
    private final Optional<PrincipalSpecification> principal;
    private final Optional<String> comment;

    public CreateCatalog(
            Identifier catalogName,
            boolean notExists,
            Identifier connectorName,
            List<Property> properties,
            Optional<PrincipalSpecification> principal,
            Optional<String> comment)
    {
        this(Optional.empty(), catalogName, notExists, connectorName, properties, principal, comment);
    }

    public CreateCatalog(
            NodeLocation location,
            Identifier catalogName,
            boolean notExists,
            Identifier connectorName,
            List<Property> properties,
            Optional<PrincipalSpecification> principal,
            Optional<String> comment)
    {
        this(Optional.of(location), catalogName, notExists, connectorName, properties, principal, comment);
    }

    private CreateCatalog(
            Optional<NodeLocation> location,
            Identifier catalogName,
            boolean notExists,
            Identifier connectorName,
            List<Property> properties,
            Optional<PrincipalSpecification> principal,
            Optional<String> comment)
    {
        super(location);
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.notExists = notExists;
        this.connectorName = requireNonNull(connectorName, "connectorName is null");
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
        this.principal = requireNonNull(principal, "principal is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public Identifier getCatalogName()
    {
        return catalogName;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public Identifier getConnectorName()
    {
        return connectorName;
    }

    public Optional<PrincipalSpecification> getPrincipal()
    {
        return principal;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateCatalog(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.copyOf(properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, notExists, properties, comment);
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
        CreateCatalog o = (CreateCatalog) obj;
        return Objects.equals(catalogName, o.catalogName) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(properties, o.properties) &&
                Objects.equals(comment, o.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("notExists", notExists)
                .add("properties", properties)
                .add("comment", comment)
                .toString();
    }
}
