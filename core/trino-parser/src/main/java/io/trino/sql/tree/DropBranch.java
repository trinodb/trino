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

public class DropBranch
        extends Statement
{
    private final QualifiedName tableName;
    private final boolean exists;
    private final Identifier branchName;

    public DropBranch(NodeLocation location, QualifiedName tableName, boolean exists, Identifier branchName)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.exists = exists;
        this.branchName = requireNonNull(branchName, "branchName is null");
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public boolean isExists()
    {
        return exists;
    }

    public Identifier getBranchName()
    {
        return branchName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDropBranch(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, exists, branchName);
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
        DropBranch o = (DropBranch) obj;
        return Objects.equals(tableName, o.tableName) &&
                exists == o.exists &&
                Objects.equals(branchName, o.branchName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("exists", exists)
                .add("branchName", branchName)
                .toString();
    }
}
