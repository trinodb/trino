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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.deltalake.DeltaHiveTypeTranslator.toHiveType;
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

    private final String name;
    private final Type type;
    private final OptionalInt fieldId;
    // Hold field names in Parquet files
    // The value is same as 'name' when the column mapping mode is none
    // The value is same as 'delta.columnMapping.physicalName' when the column mapping mode is id or name. e.g. col-6707cc9e-f3aa-4e6b-b8ef-1b03d3475680
    private final String physicalName;
    // Hold type in Parquet files
    // The value is same as 'type' when the column mapping mode is none
    // The value is same as 'delta.columnMapping.physicalName' when the column mapping mode is id or name. e.g. row(col-5924c8b3-04cf-4146-abb5-2c229e7ff708 integer)
    private final Type physicalType;
    private final DeltaLakeColumnType columnType;

    @JsonCreator
    public DeltaLakeColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("fieldId") OptionalInt fieldId,
            @JsonProperty("physicalName") String physicalName,
            @JsonProperty("physicalType") Type physicalType,
            @JsonProperty("columnType") DeltaLakeColumnType columnType)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.fieldId = requireNonNull(fieldId, "fieldId is null");
        this.physicalName = requireNonNull(physicalName, "physicalName is null");
        this.physicalType = requireNonNull(physicalType, "physicalType is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public OptionalInt getFieldId()
    {
        return fieldId;
    }

    @JsonProperty
    public String getPhysicalName()
    {
        return physicalName;
    }

    @JsonProperty
    public Type getPhysicalType()
    {
        return physicalType;
    }

    @JsonProperty
    public DeltaLakeColumnType getColumnType()
    {
        return columnType;
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
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.fieldId, other.fieldId) &&
                Objects.equals(this.physicalName, other.physicalName) &&
                Objects.equals(this.physicalType, other.physicalType) &&
                this.columnType == other.columnType;
    }

    public long getRetainedSizeInBytes()
    {
        // type is not accounted for as the instances are cached (by TypeRegistry) and shared
        return INSTANCE_SIZE + estimatedSizeOf(name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, fieldId, physicalName, physicalType, columnType);
    }

    @Override
    public String toString()
    {
        return name + ":" + type.getDisplayName() + ":" + columnType;
    }

    public HiveColumnHandle toHiveColumnHandle()
    {
        return new HiveColumnHandle(
                physicalName, // this name is used for accessing Parquet files, so it should be physical name
                0, // hiveColumnIndex; we provide fake value because we always find columns by name
                toHiveType(physicalType),
                physicalType,
                Optional.empty(),
                columnType.toHiveColumnType(),
                Optional.empty());
    }

    public static DeltaLakeColumnHandle pathColumnHandle()
    {
        return new DeltaLakeColumnHandle(PATH_COLUMN_NAME, PATH_TYPE, OptionalInt.empty(), PATH_COLUMN_NAME, PATH_TYPE, SYNTHESIZED);
    }

    public static DeltaLakeColumnHandle fileSizeColumnHandle()
    {
        return new DeltaLakeColumnHandle(FILE_SIZE_COLUMN_NAME, FILE_SIZE_TYPE, OptionalInt.empty(), FILE_SIZE_COLUMN_NAME, FILE_SIZE_TYPE, SYNTHESIZED);
    }

    public static DeltaLakeColumnHandle fileModifiedTimeColumnHandle()
    {
        return new DeltaLakeColumnHandle(FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_TYPE, OptionalInt.empty(), FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_TYPE, SYNTHESIZED);
    }
}
