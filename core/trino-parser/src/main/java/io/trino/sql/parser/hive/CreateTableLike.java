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
package io.trino.sql.parser.hive;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateTableLike
        extends Statement
{
    private final QualifiedName name;
    private final QualifiedName source;
    private final boolean notExists;

    public CreateTableLike(Optional<NodeLocation> location, QualifiedName name, QualifiedName source, boolean notExists)
    {
        super(location);
        this.name = requireNonNull(name, "table is null");
        this.source = requireNonNull(source, "elements is null");
        this.notExists = notExists;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public QualifiedName getSource()
    {
        return source;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTableLike(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, source, notExists);
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
        CreateTableLike o = (CreateTableLike) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(source, o.source) &&
                Objects.equals(notExists, o.notExists);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("source", source)
                .add("notExists", notExists)
                .toString();
    }
}
