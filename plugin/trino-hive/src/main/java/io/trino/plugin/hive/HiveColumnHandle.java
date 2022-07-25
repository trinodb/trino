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
package io.trino.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.SizeOf;
import io.trino.plugin.hive.metastore.Column;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.HiveUpdateProcessor.getUpdateRowIdColumnHandle;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_ROW_ID_ROW_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * ColumnHandle for Hive Connector representing a full top level column or a projected column. Currently projected columns
 * that represent a simple chain of dereferences are supported. e.g. for a column "A" with type struct(B struct(C bigint, ...), ....)
 * there can be a projected column representing expression "A.B.C".
 */
public class HiveColumnHandle
        implements ColumnHandle
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(HiveColumnHandle.class).instanceSize();

    public static final int PATH_COLUMN_INDEX = -11;
    public static final String PATH_COLUMN_NAME = "$path";
    public static final HiveType PATH_HIVE_TYPE = HIVE_STRING;
    public static final Type PATH_TYPE = VARCHAR;

    public static final int BUCKET_COLUMN_INDEX = -12;
    public static final String BUCKET_COLUMN_NAME = "$bucket";
    public static final HiveType BUCKET_HIVE_TYPE = HIVE_INT;
    public static final Type BUCKET_TYPE_SIGNATURE = INTEGER;

    public static final int FILE_SIZE_COLUMN_INDEX = -13;
    public static final String FILE_SIZE_COLUMN_NAME = "$file_size";
    public static final HiveType FILE_SIZE_TYPE = HIVE_LONG;
    public static final Type FILE_SIZE_TYPE_SIGNATURE = BIGINT;

    public static final int FILE_MODIFIED_TIME_COLUMN_INDEX = -14;
    public static final String FILE_MODIFIED_TIME_COLUMN_NAME = "$file_modified_time";
    public static final HiveType FILE_MODIFIED_TIME_TYPE = HiveType.HIVE_TIMESTAMP;
    public static final Type FILE_MODIFIED_TIME_TYPE_SIGNATURE = TIMESTAMP_TZ_MILLIS;

    public static final int PARTITION_COLUMN_INDEX = -15;
    public static final String PARTITION_COLUMN_NAME = "$partition";
    public static final HiveType PARTITION_HIVE_TYPE = HIVE_STRING;
    public static final Type PARTITION_TYPE_SIGNATURE = VARCHAR;

    public static final int UPDATE_ROW_ID_COLUMN_INDEX = -16;
    public static final String UPDATE_ROW_ID_COLUMN_NAME = "$row_id";

    public enum ColumnType
    {
        PARTITION_KEY,
        REGULAR,
        SYNTHESIZED,
    }

    // Information about top level hive column
    private final String baseColumnName;
    private final int baseHiveColumnIndex;
    private final HiveType baseHiveType;
    private final Type baseType;
    private final Optional<String> comment;

    // Information about parts of the base column to be referenced by this column handle.
    private final Optional<HiveColumnProjectionInfo> hiveColumnProjectionInfo;

    private final String name;
    private final ColumnType columnType;

    @JsonCreator
    public HiveColumnHandle(
            @JsonProperty("baseColumnName") String baseColumnName,
            @JsonProperty("baseHiveColumnIndex") int baseHiveColumnIndex,
            @JsonProperty("baseHiveType") HiveType baseHiveType,
            @JsonProperty("baseType") Type baseType,
            @JsonProperty("hiveColumnProjectionInfo") Optional<HiveColumnProjectionInfo> hiveColumnProjectionInfo,
            @JsonProperty("columnType") ColumnType columnType,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.baseColumnName = requireNonNull(baseColumnName, "baseColumnName is null");
        checkArgument(baseHiveColumnIndex >= 0 || columnType == PARTITION_KEY || columnType == SYNTHESIZED, "baseHiveColumnIndex is negative");
        this.baseHiveColumnIndex = baseHiveColumnIndex;
        this.baseHiveType = requireNonNull(baseHiveType, "baseHiveType is null");
        this.baseType = requireNonNull(baseType, "baseType is null");

        this.hiveColumnProjectionInfo = requireNonNull(hiveColumnProjectionInfo, "hiveColumnProjectionInfo is null");

        this.name = this.baseColumnName + hiveColumnProjectionInfo.map(HiveColumnProjectionInfo::getPartialName).orElse("");

        this.columnType = requireNonNull(columnType, "columnType is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public static HiveColumnHandle createBaseColumn(
            String topLevelColumnName,
            int topLevelColumnIndex,
            HiveType hiveType,
            Type type,
            ColumnType columnType,
            Optional<String> comment)
    {
        return new HiveColumnHandle(topLevelColumnName, topLevelColumnIndex, hiveType, type, Optional.empty(), columnType, comment);
    }

    public HiveColumnHandle getBaseColumn()
    {
        return isBaseColumn() ? this : createBaseColumn(baseColumnName, baseHiveColumnIndex, baseHiveType, baseType, columnType, comment);
    }

    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getBaseColumnName()
    {
        return baseColumnName;
    }

    @JsonProperty
    public HiveType getBaseHiveType()
    {
        return baseHiveType;
    }

    @JsonProperty
    public Type getBaseType()
    {
        return baseType;
    }

    @JsonProperty
    public int getBaseHiveColumnIndex()
    {
        return baseHiveColumnIndex;
    }

    @JsonProperty
    public Optional<HiveColumnProjectionInfo> getHiveColumnProjectionInfo()
    {
        return hiveColumnProjectionInfo;
    }

    public HiveType getHiveType()
    {
        return hiveColumnProjectionInfo.map(HiveColumnProjectionInfo::getHiveType).orElse(baseHiveType);
    }

    public Type getType()
    {
        return hiveColumnProjectionInfo.map(HiveColumnProjectionInfo::getType).orElse(baseType);
    }

    public boolean isPartitionKey()
    {
        return columnType == PARTITION_KEY;
    }

    public boolean isHidden()
    {
        return columnType == SYNTHESIZED;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(getType())
                .setHidden(isHidden())
                .build();
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public ColumnType getColumnType()
    {
        return columnType;
    }

    public boolean isBaseColumn()
    {
        return hiveColumnProjectionInfo.isEmpty();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(baseColumnName, baseHiveColumnIndex, baseHiveType, baseType, hiveColumnProjectionInfo, columnType, comment);
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
        HiveColumnHandle other = (HiveColumnHandle) obj;
        return Objects.equals(this.baseColumnName, other.baseColumnName) &&
                Objects.equals(this.baseHiveColumnIndex, other.baseHiveColumnIndex) &&
                Objects.equals(this.baseHiveType, other.baseHiveType) &&
                Objects.equals(this.baseType, other.baseType) &&
                Objects.equals(this.hiveColumnProjectionInfo, other.hiveColumnProjectionInfo) &&
                Objects.equals(this.name, other.name) &&
                this.columnType == other.columnType &&
                Objects.equals(this.comment, other.comment);
    }

    @Override
    public String toString()
    {
        return name + ":" + getHiveType() + ":" + columnType;
    }

    public Column toMetastoreColumn()
    {
        return new Column(name, getHiveType(), comment);
    }

    public static HiveColumnHandle getDeleteRowIdColumnHandle()
    {
        return createBaseColumn(UPDATE_ROW_ID_COLUMN_NAME, UPDATE_ROW_ID_COLUMN_INDEX, toHiveType(ACID_ROW_ID_ROW_TYPE), ACID_ROW_ID_ROW_TYPE, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle updateRowIdColumnHandle(List<HiveColumnHandle> columnHandles, List<ColumnHandle> updatedColumns)
    {
        requireNonNull(updatedColumns, "updatedColumns is null");
        List<HiveColumnHandle> nonUpdatedColumnHandles = columnHandles.stream()
                .filter(column -> !column.isPartitionKey() && !column.isHidden() && !updatedColumns.contains(column))
                .collect(toImmutableList());
        return getUpdateRowIdColumnHandle(nonUpdatedColumnHandles);
    }

    public static HiveColumnHandle pathColumnHandle()
    {
        return createBaseColumn(PATH_COLUMN_NAME, PATH_COLUMN_INDEX, PATH_HIVE_TYPE, PATH_TYPE, SYNTHESIZED, Optional.empty());
    }

    /**
     * The column indicating the bucket id.
     * When table bucketing differs from partition bucketing, this column indicates
     * what bucket the row will fall in under the table bucketing scheme.
     */
    public static HiveColumnHandle bucketColumnHandle()
    {
        return createBaseColumn(BUCKET_COLUMN_NAME, BUCKET_COLUMN_INDEX, BUCKET_HIVE_TYPE, BUCKET_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle fileSizeColumnHandle()
    {
        return createBaseColumn(FILE_SIZE_COLUMN_NAME, FILE_SIZE_COLUMN_INDEX, FILE_SIZE_TYPE, FILE_SIZE_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle fileModifiedTimeColumnHandle()
    {
        return createBaseColumn(FILE_MODIFIED_TIME_COLUMN_NAME, FILE_MODIFIED_TIME_COLUMN_INDEX, FILE_MODIFIED_TIME_TYPE, FILE_MODIFIED_TIME_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static HiveColumnHandle partitionColumnHandle()
    {
        return createBaseColumn(PARTITION_COLUMN_NAME, PARTITION_COLUMN_INDEX, PARTITION_HIVE_TYPE, PARTITION_TYPE_SIGNATURE, SYNTHESIZED, Optional.empty());
    }

    public static boolean isPathColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == PATH_COLUMN_INDEX;
    }

    public static boolean isBucketColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == BUCKET_COLUMN_INDEX;
    }

    public static boolean isFileSizeColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == FILE_SIZE_COLUMN_INDEX;
    }

    public static boolean isFileModifiedTimeColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == FILE_MODIFIED_TIME_COLUMN_INDEX;
    }

    public static boolean isPartitionColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == PARTITION_COLUMN_INDEX;
    }

    public static boolean isRowIdColumnHandle(HiveColumnHandle column)
    {
        return column.getBaseHiveColumnIndex() == UPDATE_ROW_ID_COLUMN_INDEX;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(baseColumnName)
                + baseHiveType.getRetainedSizeInBytes()
                // baseType is not accounted for as the instances are cached (by TypeRegistry) and shared
                + sizeOf(comment, SizeOf::estimatedSizeOf)
                + sizeOf(hiveColumnProjectionInfo, HiveColumnProjectionInfo::getRetainedSizeInBytes)
                + estimatedSizeOf(name);
    }
}
