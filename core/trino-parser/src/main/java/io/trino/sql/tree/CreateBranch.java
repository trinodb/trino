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

public final class CreateBranch
        extends Statement
{
    private final QualifiedName tableName;
    private final SaveMode saveMode;
    private final Identifier branchName;
    private final Optional<Identifier> fromBranch;
    private final List<Property> properties;

    public CreateBranch(NodeLocation location, QualifiedName tableName, Identifier branchName, Optional<Identifier> fromBranch, SaveMode saveMode, List<Property> properties)
    {
        super(location);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.saveMode = requireNonNull(saveMode, "saveMode is null");
        this.branchName = requireNonNull(branchName, "branchName is null");
        this.fromBranch = requireNonNull(fromBranch, "fromBranch is null");
        this.properties = ImmutableList.copyOf(properties);
    }

    public QualifiedName getTableName()
    {
        return tableName;
    }

    public Identifier getBranchName()
    {
        return branchName;
    }

    public Optional<Identifier> getFromBranch()
    {
        return fromBranch;
    }

    public SaveMode getSaveMode()
    {
        return saveMode;
    }

    public List<Property> getProperties()
    {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateBranch(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(properties)
                .build();
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
        CreateBranch o = (CreateBranch) obj;
        return Objects.equals(tableName, o.tableName) &&
                Objects.equals(branchName, o.branchName) &&
                Objects.equals(fromBranch, o.fromBranch) &&
                saveMode == o.saveMode &&
                Objects.equals(properties, o.properties);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, branchName, fromBranch, saveMode, properties);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableName", tableName)
                .add("branchName", branchName)
                .add("fromBranch", fromBranch)
                .add("saveMode", saveMode)
                .add("properties", properties)
                .toString();
    }
}
