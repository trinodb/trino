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

public final class Comment
        extends Statement
{
    public enum Type
    {
        TABLE
    }

    private final Type type;
    private final QualifiedName name;
    private final Optional<String> comment;

    public Comment(Type type, QualifiedName name, Optional<String> comment)
    {
        this(Optional.empty(), type, name, comment);
    }

    public Comment(NodeLocation location, Type type, QualifiedName name, Optional<String> comment)
    {
        this(Optional.of(location), type, name, comment);
    }

    private Comment(Optional<NodeLocation> location, Type type, QualifiedName name, Optional<String> comment)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "table is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public Type getType()
    {
        return type;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitComment(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name, comment);
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
        Comment o = (Comment) obj;
        return Objects.equals(type, o.type) &&
                Objects.equals(name, o.name) &&
                Objects.equals(comment, o.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .add("comment", comment)
                .toString();
    }
}
