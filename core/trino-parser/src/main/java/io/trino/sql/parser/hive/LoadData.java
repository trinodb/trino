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

import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * @author tangyun@bigo.sg
 * @date 1/3/20 8:14 PM
 */
public class LoadData
        extends Statement
{
    private final QualifiedName name;
    private final String path;
    private final boolean overwrite;
    private final List<SingleColumn> partitions;

    public LoadData(Optional<NodeLocation> location, QualifiedName name,
                    String path, boolean overwrite, List<SingleColumn> partitions)
    {
        super(location);
        this.name = name;
        this.path = path;
        this.overwrite = overwrite;
        this.partitions = partitions;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public String getPath()
    {
        return path;
    }

    public boolean isOverwrite()
    {
        return overwrite;
    }

    public List<SingleColumn> getPartitions()
    {
        return partitions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLoadData(this, context);
    }

    @Override
    public List<SingleColumn> getChildren()
    {
        return partitions;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, path, overwrite, partitions);
    }

    // for test
    public String getPartitionEnd()
    {
        StringBuilder stringBuilder = new StringBuilder();
        if (partitions.size() > 0) {
            stringBuilder.append("/");
            partitions.stream().forEach(singleColumn -> {
                stringBuilder.append(singleColumn.getAlias().get().getValue().replaceAll("'", ""));
                stringBuilder.append("=");
                stringBuilder.append(singleColumn.getExpression().toString().replaceAll("'", ""));
                stringBuilder.append("/");
            });
        }
        return stringBuilder.toString();
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
        LoadData o = (LoadData) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(path, o.path) &&
                Objects.equals(partitions, o.partitions) &&
                Objects.equals(overwrite, o.overwrite);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("path", path)
                .add("overwrite", overwrite)
                .add("partitions", partitions)
                .toString();
    }
}
