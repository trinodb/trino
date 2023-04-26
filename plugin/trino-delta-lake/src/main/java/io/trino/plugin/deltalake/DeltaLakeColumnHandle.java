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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.SYNTHESIZED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = instanceSize(DeltaLakeColumnHandle.class);

    public static final String ROW_ID_COLUMN_NAME = "$row_id";

    public static final Type MERGE_ROW_ID_TYPE = rowType(
            field("path", VARCHAR),
            field("position", BIGINT),
            field("partition", VARCHAR));

    public static final String PATH_COLUMN_NAME = "$path";
    public static final Type PATH_TYPE = VARCHAR;

    public static final String FILE_SIZE_COLUMN_NAME = "$file_size";
    public static final Type FILE_SIZE_TYPE = BIGINT;

    public static final String FILE_MODIFIED_TIME_COLUMN_NAME = "$file_modified_time";
    public static final Type FILE_MODIFIED_TIME_TYPE = TIMESTAMP_TZ_MILLIS;

    private final String baseColumnName;
    private final Type baseType;
    private final OptionalInt baseFieldId;
    // Hold field names in Parquet files
    // The value is same as 'name' when the column mapping mode is none
    // The value is same as 'delta.columnMapping.physicalName' when the column mapping mode is id or name. e.g. col-6707cc9e-f3aa-4e6b-b8ef-1b03d3475680
    private final String basePhysicalColumnName;
    // Hold type in Parquet files
    // The value is same as 'type' when the column mapping mode is none
    // The value is same as 'delta.columnMapping.physicalName' when the column mapping mode is id or name. e.g. row(col-5924c8b3-04cf-4146-abb5-2c229e7ff708 integer)
    private final Type basePhysicalType;
    private final DeltaLakeColumnType columnType;
    private final Optional<DeltaLakeColumnProjectionInfo> projectionInfo;

    @JsonCreator
    public DeltaLakeColumnHandle(
            @JsonProperty("baseColumnName") String baseColumnName,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("baseFieldId") OptionalInt baseFieldId,
            @JsonProperty("basePhysicalColumnName") String basePhysicalColumnName,
            @JsonProperty("basePhysicalType") Type basePhysicalType,
            @JsonProperty("columnType") DeltaLakeColumnType columnType,
            @JsonProperty("projectionInfo") Optional<DeltaLakeColumnProjectionInfo> projectionInfo)
    {
        this.baseColumnName = requireNonNull(baseColumnName, "baseColumnName is null");
        this.baseType = requireNonNull(baseType, "baseType is null");
        this.baseFieldId = requireNonNull(baseFieldId, "baseFieldId is null");
        this.basePhysicalColumnName = requireNonNull(basePhysicalColumnName, "basePhysicalColumnName is null");
        this.basePhysicalType = requireNonNull(basePhysicalType, "basePhysicalType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        checkArgument(projectionInfo.isEmpty() || columnType == REGULAR, "Projection info present for column type: %s", columnType);
        this.projectionInfo = projectionInfo;
    }

    @JsonProperty
    public String getBaseColumnName()
    {
        return baseColumnName;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    @JsonProperty
    public OptionalInt getBaseFieldId()
    {
        return baseFieldId;
    }

    @JsonProperty
    public String getBasePhysicalColumnName()
    {
        return basePhysicalColumnName;
    }

    @JsonProperty
    public Type getBasePhysicalType()
    {
        return basePhysicalType;
    }

    @JsonProperty
    public DeltaLakeColumnType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public Optional<DeltaLakeColumnProjectionInfo> getProjectionInfo()
    {
        return projectionInfo;
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
        DeltaLakeColumnHandle other = (DeltaLakeColumnHandle) obj;
        return Objects.equals(this.baseColumnName, other.baseColumnName) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.baseFieldId, other.baseFieldId) &&
                Objects.equals(this.basePhysicalColumnName, other.basePhysicalColumnName) &&
                Objects.equals(this.basePhysicalType, other.basePhysicalType) &&
                this.columnType == other.columnType &&
                Objects.equals(this.projectionInfo, other.projectionInfo);
    }

    @JsonIgnore
    public String getColumnName()
    {
        checkState(isBaseColumn(), "Unexpected dereference: %s", this);
        return baseColumnName;
    }

    @JsonIgnore
    public String getQualifiedPhysicalName()
    {
        return projectionInfo.map(projectionInfo -> basePhysicalColumnName + "#" + projectionInfo.getPartialName())
                .orElse(basePhysicalColumnName);
    }

    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE
                + estimatedSizeOf(baseColumnName)
                + sizeOf(baseFieldId)
                + estimatedSizeOf(basePhysicalColumnName)
                + projectionInfo.map(DeltaLakeColumnProjectionInfo::getRetainedSizeInBytes).orElse(0L);
    }

    @JsonIgnore
    public boolean isBaseColumn()
    {
        return projectionInfo.isEmpty();
    }

    @JsonIgnore
    public Type getType()
    {
        return projectionInfo.map(DeltaLakeColumnProjectionInfo::getType)
                .orElse(baseType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseColumnName, baseType, baseFieldId, basePhysicalColumnName, basePhysicalType, columnType, projectionInfo);
    }

    @Override
    public String toString()
    {
        return getQualifiedPhysicalName() +
                ":" + projectionInfo.map(DeltaLakeColumnProjectionInfo::getType).orElse(baseType).getDisplayName() +
                ":" + columnType;
    }

    public HiveColumnHandle toHiveColumnHandle()
    {
        return new HiveColumnHandle(
                basePhysicalColumnName, // this name is used for accessing Parquet files, so it should be physical name
                0, // hiveColumnIndex; we provide fake value because we always find columns by name
                toHiveType(basePhysicalType),
                basePhysicalType,
                projectionInfo.map(DeltaLakeColumnProjectionInfo::toHiveColumnProjectionInfo),
                columnType.toHiveColumnType(),
                Optional.empty());
    }

    public static DeltaLakeColumnHandle pathColumnHandle()
    {
        return new DeltaLakeColumnHandle(PATH_COLUMN_NAME, PATH_TYPE, OptionalInt.empty(), PATH_COLUMN_NAME, PATH_TYPE, SYNTHESIZED, Optional.empty());
    }

    public static DeltaLakeColumnHandle fileSizeColumnHandle()
    {
        return new DeltaLakeColumnHandle(FILE_SIZE_COLUMN_NAME, FILE_SIZE_TYPE, OptionalInt.empty(), FILE_SIZE_COLUMN_NAME, FILE_SIZE_TYPE, SYNTHESIZED, Optional.empty());
    }

    public static DeltaLakeColumnHandle fileModifiedTimeColumnHandle()
    {
        return new DeltaLakeColumnHandle(FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_TYPE, OptionalInt.empty(), FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_TYPE, SYNTHESIZED, Optional.empty());
    }

    public static DeltaLakeColumnHandle mergeRowIdColumnHandle()
    {
        return new DeltaLakeColumnHandle(ROW_ID_COLUMN_NAME, MERGE_ROW_ID_TYPE, OptionalInt.empty(), ROW_ID_COLUMN_NAME, MERGE_ROW_ID_TYPE, SYNTHESIZED, Optional.empty());
    }
}
