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
import com.google.common.collect.Iterables;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_PATH;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.PARTITION;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.IS_DELETED;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;

public class IcebergColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergColumnHandle.class);

    // Iceberg reserved row ids begin at INTEGER.MAX_VALUE and count down. Starting with MIN_VALUE here to avoid conflicts.
    public static final int TRINO_MERGE_ROW_ID = Integer.MIN_VALUE;
    public static final String TRINO_ROW_ID_NAME = "$row_id";

    public static final int TRINO_MERGE_PARTITION_SPEC_ID = Integer.MIN_VALUE + 1;
    public static final int TRINO_MERGE_PARTITION_DATA = Integer.MIN_VALUE + 2;

    public static final String DATA_CHANGE_TYPE_NAME = "_change_type";
    public static final int DATA_CHANGE_TYPE_ID = Integer.MIN_VALUE + 3;
    public static final String DATA_CHANGE_VERSION_NAME = "_change_version_id";
    public static final int DATA_CHANGE_VERSION_ID = Integer.MIN_VALUE + 4;
    public static final String DATA_CHANGE_TIMESTAMP_NAME = "_change_timestamp";
    public static final int DATA_CHANGE_TIMESTAMP_ID = Integer.MIN_VALUE + 5;
    public static final String DATA_CHANGE_ORDINAL_NAME = "_change_ordinal";
    public static final int DATA_CHANGE_ORDINAL_ID = Integer.MIN_VALUE + 6;

    private final ColumnIdentity baseColumnIdentity;
    private final Type baseType;
    // The list of field ids to indicate the projected part of the top-level column represented by baseColumnIdentity
    private final List<Integer> path;
    private final Type type;
    private final boolean nullable;
    private final Optional<String> comment;
    // Cache of ColumnIdentity#getId to ensure quick access, even with dereferences
    private final int id;

    @JsonCreator
    public IcebergColumnHandle(
            @JsonProperty("baseColumnIdentity") ColumnIdentity baseColumnIdentity,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("path") List<Integer> path,
            @JsonProperty("type") Type type,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.baseColumnIdentity = requireNonNull(baseColumnIdentity, "baseColumnIdentity is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.path = ImmutableList.copyOf(requireNonNull(path, "path is null"));
        this.type = requireNonNull(type, "type is null");
        this.nullable = nullable;
        this.comment = requireNonNull(comment, "comment is null");
        this.id = path.isEmpty() ? baseColumnIdentity.getId() : Iterables.getLast(path);
    }

    @JsonIgnore
    public ColumnIdentity getColumnIdentity()
    {
        ColumnIdentity columnIdentity = baseColumnIdentity;
        for (int fieldId : path) {
            columnIdentity = columnIdentity.getChildByFieldId(fieldId);
        }
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

    @JsonIgnore
    public IcebergColumnHandle getBaseColumn()
    {
        return new IcebergColumnHandle(getBaseColumnIdentity(), getBaseType(), ImmutableList.of(), getBaseType(), isNullable(), Optional.empty());
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonIgnore
    public int getId()
    {
        return id;
    }

    /**
     * For nested columns, this is the unqualified name of the last field in the path
     */
    @JsonIgnore
    public String getName()
    {
        return getColumnIdentity().getName();
    }

    @JsonProperty
    public List<Integer> getPath()
    {
        return path;
    }

    /**
     * The dot separated path components used to address this column, including all dereferences and the column name.
     */
    @JsonIgnore
    public String getQualifiedName()
    {
        ImmutableList.Builder<String> pathNames = ImmutableList.builder();
        ColumnIdentity columnIdentity = baseColumnIdentity;
        pathNames.add(columnIdentity.getName());
        for (int fieldId : path) {
            columnIdentity = columnIdentity.getChildByFieldId(fieldId);
            pathNames.add(columnIdentity.getName());
        }
        // Iceberg tables are guaranteed not to have ambiguous column names so joining them like this must uniquely identify a single column.
        return String.join(".", pathNames.build());
    }

    @JsonIgnore
    public boolean isBaseColumn()
    {
        return path.isEmpty();
    }

    @JsonIgnore
    public boolean isRowPositionColumn()
    {
        return id == ROW_POSITION.fieldId();
    }

    @JsonIgnore
    public boolean isMergeRowIdColumn()
    {
        return id == TRINO_MERGE_ROW_ID;
    }

    /**
     * Marker column used by the Iceberg DeleteFilter to indicate rows which are deleted by equality deletes.
     */
    @JsonIgnore
    public boolean isIsDeletedColumn()
    {
        return id == IS_DELETED.fieldId();
    }

    @JsonIgnore
    public boolean isPartitionColumn()
    {
        return id == PARTITION.getId();
    }

    @JsonIgnore
    public boolean isFileModifiedTimeColumn()
    {
        return id == FILE_MODIFIED_TIME.getId();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseColumnIdentity, baseType, path, type, nullable, comment);
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
        return Objects.equals(this.baseColumnIdentity, other.baseColumnIdentity) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.path, other.path) &&
                Objects.equals(this.type, other.type) &&
                this.nullable == other.nullable &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return getId() + ":" + getName() + ":" + type.getDisplayName();
    }

    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + baseColumnIdentity.getRetainedSizeInBytes()
                + estimatedSizeOf(path, SizeOf::sizeOf)
                + sizeOf(nullable)
                + sizeOf(comment, SizeOf::estimatedSizeOf)
                + sizeOf(id);
    }

    public static IcebergColumnHandle partitionColumnHandle()
    {
        return new IcebergColumnHandle(
                columnIdentity(PARTITION),
                PARTITION.getType(),
                ImmutableList.of(),
                PARTITION.getType(),
                false,
                Optional.empty());
    }

    public static ColumnMetadata partitionColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(PARTITION.getColumnName())
                .setType(PARTITION.getType())
                .setHidden(true)
                .build();
    }

    public static IcebergColumnHandle pathColumnHandle()
    {
        return new IcebergColumnHandle(
                columnIdentity(FILE_PATH),
                FILE_PATH.getType(),
                ImmutableList.of(),
                FILE_PATH.getType(),
                false,
                Optional.empty());
    }

    public static ColumnMetadata pathColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(FILE_PATH.getColumnName())
                .setType(FILE_PATH.getType())
                .setHidden(true)
                .build();
    }

    public static IcebergColumnHandle fileModifiedTimeColumnHandle()
    {
        return new IcebergColumnHandle(
                columnIdentity(FILE_MODIFIED_TIME),
                FILE_MODIFIED_TIME.getType(),
                ImmutableList.of(),
                FILE_MODIFIED_TIME.getType(),
                false,
                Optional.empty());
    }

    public static ColumnMetadata fileModifiedTimeColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(FILE_MODIFIED_TIME.getColumnName())
                .setType(FILE_MODIFIED_TIME.getType())
                .setHidden(true)
                .build();
    }

    private static ColumnIdentity columnIdentity(IcebergMetadataColumn metadata)
    {
        return new ColumnIdentity(metadata.getId(), metadata.getColumnName(), metadata.getTypeCategory(), ImmutableList.of());
    }

    public boolean isPathColumn()
    {
        return getColumnIdentity().getId() == FILE_PATH.getId();
    }
}
