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

public class CreateTag
        extends Statement
{
    private final QualifiedName table;
    private final Identifier tagName;
    private final boolean replace;
    private final boolean notExists;
    private final Optional<Expression> snapshotId;
    private final Optional<Expression> retentionDays;

    public CreateTag(
            NodeLocation location,
            QualifiedName table,
            Identifier tagName,
            boolean replace,
            boolean notExists,
            Optional<Expression> snapshotId,
            Optional<Expression> retentionDays)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.tagName = requireNonNull(tagName, "tagName is null");
        this.replace = replace;
        this.notExists = notExists;
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.retentionDays = requireNonNull(retentionDays, "retentionDays is null");
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Identifier getTagName()
    {
        return tagName;
    }

    public boolean isReplace()
    {
        return replace;
    }

    public boolean isNotExists()
    {
        return notExists;
    }

    public Optional<Expression> getSnapshotId()
    {
        return snapshotId;
    }

    public Optional<Expression> getRetentionDays()
    {
        return retentionDays;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTag(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        snapshotId.ifPresent(children::add);
        retentionDays.ifPresent(children::add);
        return children.build();
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
        CreateTag createTag = (CreateTag) o;
        return replace == createTag.replace &&
                notExists == createTag.notExists &&
                Objects.equals(table, createTag.table) &&
                Objects.equals(tagName, createTag.tagName) &&
                Objects.equals(snapshotId, createTag.snapshotId) &&
                Objects.equals(retentionDays, createTag.retentionDays);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tagName, replace, notExists, snapshotId, retentionDays);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tagName", tagName)
                .add("replace", replace)
                .add("notExists", notExists)
                .add("snapshotId", snapshotId)
                .add("retentionDays", retentionDays)
                .toString();
    }
}
