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

public final class FastForwardBranch
        extends Statement
{
    private final QualifiedName tableName;
    private final Identifier sourceBranchName;
    private final Identifier targetBranchName;

    public FastForwardBranch(NodeLocation location, QualifiedName tableName, Identifier sourceBranchName, Identifier targetBranchName)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.sourceBranchName = requireNonNull(sourceBranchName, "sourceBranchName is null");
        this.targetBranchName = requireNonNull(targetBranchName, "targetBranchName is null");
    }

    public QualifiedName geTableName()
    {
        return tableName;
    }

    public Identifier getSourceBranchName()
    {
        return sourceBranchName;
    }

    public Identifier getTargetBranchName()
    {
        return targetBranchName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFastForwardBranch(this, context);
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
        FastForwardBranch o = (FastForwardBranch) obj;
        return Objects.equals(tableName, o.tableName) &&
                Objects.equals(sourceBranchName, o.sourceBranchName) &&
                Objects.equals(targetBranchName, o.targetBranchName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, sourceBranchName, targetBranchName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("sourceBranchName", sourceBranchName)
                .add("targetBranchName", targetBranchName)
                .toString();
    }
}
