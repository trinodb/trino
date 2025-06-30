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

public class ReplaceTag
        extends Statement
{
    private final QualifiedName table;
    private final Identifier tagName;
    private final Optional<Expression> snapshotId;
    private final Optional<Expression> retentionDays;

    public ReplaceTag(
            NodeLocation location,
            QualifiedName table,
            Identifier tagName,
            Optional<Expression> snapshotId,
            Optional<Expression> retentionDays)
    {
        super(location);
        this.table = requireNonNull(table, "table is null");
        this.tagName = requireNonNull(tagName, "tagName is null");
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
        return visitor.visitReplaceTag(this, context);
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
        ReplaceTag that = (ReplaceTag) o;
        return Objects.equals(table, that.table) &&
                Objects.equals(tagName, that.tagName) &&
                Objects.equals(snapshotId, that.snapshotId) &&
                Objects.equals(retentionDays, that.retentionDays);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, tagName, snapshotId, retentionDays);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tagName", tagName)
                .add("snapshotId", snapshotId)
                .add("retentionDays", retentionDays)
                .toString();
    }
}
