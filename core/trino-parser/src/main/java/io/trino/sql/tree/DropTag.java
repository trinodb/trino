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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropTag
        extends Statement
{
    private final QualifiedName table;
    private final Identifier tagName;

    public DropTag(NodeLocation location, QualifiedName table, Identifier tagName)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.tagName = requireNonNull(tagName, "tagName is null");
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Identifier getTagName()
    {
        return tagName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropTag(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        DropTag dropTag = (DropTag) o;
        return Objects.equals(table, dropTag.table) &&
                Objects.equals(tagName, dropTag.tagName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tagName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tagName", tagName)
                .toString();
    }
}
