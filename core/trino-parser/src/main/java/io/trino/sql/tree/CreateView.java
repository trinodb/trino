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

public class CreateView
        extends Statement
{
    public enum Security
    {
        INVOKER, DEFINER
    }

    private final QualifiedName name;
    private final Query query;
    private final SaveMode saveMode;
    private final Optional<String> comment;
    private final Optional<Security> security;
    private final List<Property> properties;

    public CreateView(NodeLocation location, QualifiedName name, Query query, SaveMode saveMode, Optional<String> comment, Optional<Security> security, List<Property> properties)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.query = requireNonNull(query, "query is null");
        this.saveMode = requireNonNull(saveMode, "saveMode is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.security = requireNonNull(security, "security is null");
        this.properties = ImmutableList.copyOf(properties);
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    public SaveMode getSaveMode()
    {
        return saveMode;
    }

    public boolean isReplace()
    {
        return saveMode == SaveMode.REPLACE;
    }

    public boolean isNotExists()
    {
        return saveMode == SaveMode.IGNORE;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    public Optional<Security> getSecurity()
    {
        return security;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateView(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(query)
                .addAll(properties)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, query, saveMode, comment, security, properties);
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
        CreateView o = (CreateView) obj;
        return Objects.equals(name, o.name)
                && Objects.equals(query, o.query)
                && saveMode == o.saveMode
                && Objects.equals(comment, o.comment)
                && Objects.equals(security, o.security)
                && Objects.equals(properties, o.properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .add("saveMode", saveMode)
                .add("comment", comment)
                .add("security", security)
                .add("properties", properties)
                .toString();
    }
}
