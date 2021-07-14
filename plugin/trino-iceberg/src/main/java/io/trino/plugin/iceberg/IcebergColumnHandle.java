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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static java.util.Objects.requireNonNull;

public class IcebergColumnHandle
        implements ColumnHandle
{
    private final ColumnIdentity columnIdentity;
    private final Type type;
    private final ColumnIdentity baseColumnIdentity;
    private final Type baseType;
    private final Optional<String> comment;
    private final List<String> path;
    private List<String> fullPath;

    @JsonCreator
    public IcebergColumnHandle(
            @JsonProperty("columnIdentity") ColumnIdentity columnIdentity,
            @JsonProperty("type") Type type,
            @JsonProperty("baseColumnIdentity") ColumnIdentity baseColumnIdentity,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("path") List<String> path)
    {
        this.columnIdentity = requireNonNull(columnIdentity, "columnIdentity is null");
        this.type = requireNonNull(type, "type is null");
        this.baseColumnIdentity = requireNonNull(baseColumnIdentity, "baseColumnIdentity is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.comment = requireNonNull(comment, "comment is null");
        this.path = requireNonNull(path, "path is null");
    }

    @JsonProperty
    public ColumnIdentity getColumnIdentity()
    {
        return columnIdentity;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public ColumnIdentity getBaseColumnIdentity()
    {
        return baseColumnIdentity;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    public IcebergColumnHandle getBaseColumn()
    {
        return new IcebergColumnHandle(getBaseColumnIdentity(), getBaseType(), getBaseColumnIdentity(), getBaseType(), Optional.empty(), ImmutableList.of());
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonIgnore
    public int getId()
    {
        return columnIdentity.getId();
    }

    @JsonIgnore
    public String getName()
    {
        return columnIdentity.getName();
    }

    @JsonProperty
    public List<String> getPath()
    {
        return path;
    }

    public List<String> getFullPath()
    {
        if (fullPath == null) {
            fullPath = ImmutableList.<String>builder()
                    .addAll(getPath())
                    .add(getName())
                    .build();
        }
        return fullPath;
    }

    public String getQualifiedName()
    {
        return String.join(".", getFullPath());
    }

    public boolean isBaseColumn()
    {
        return path.isEmpty();
    }

    public boolean isParentOf(IcebergColumnHandle otherColumn)
    {
        List<String> myPath = getFullPath();
        List<String> otherPath = otherColumn.getFullPath();
        return otherPath.size() >= myPath.size() && otherPath.subList(0, myPath.size()).equals(myPath);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIdentity, type, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IcebergColumnHandle other = (IcebergColumnHandle) obj;
        return Objects.equals(this.columnIdentity, other.columnIdentity) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return getId() + ":" + getName() + ":" + type.getDisplayName();
    }

    public static IcebergColumnHandle primitiveIcebergColumnHandle(int id, String name, Type type, Optional<String> comment, List<String> path)
    {
        return new IcebergColumnHandle(primitiveColumnIdentity(id, name), type, primitiveColumnIdentity(id, name), type, comment, path);
    }

    public static IcebergColumnHandle create(Types.NestedField column, TypeManager typeManager, List<String> path)
    {
        Type type = toTrinoType(column.type(), typeManager);
        return new IcebergColumnHandle(
                createColumnIdentity(column),
                type,
                createColumnIdentity(column),
                type,
                Optional.ofNullable(column.doc()),
                path);
    }
}
