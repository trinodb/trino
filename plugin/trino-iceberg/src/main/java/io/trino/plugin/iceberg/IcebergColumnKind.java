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

import io.trino.filesystem.TrinoInputFile;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.iceberg.Schema;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_PATH;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.PARTITION;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Objects.requireNonNull;

sealed interface IcebergColumnKind
{
    record Constant(Type type, @Nullable Object value)
            implements IcebergColumnKind
    {
        public Constant
        {
            requireNonNull(type, "type is null");
        }
    }

    record MergeRowId()
            implements IcebergColumnKind {}

    record RowPosition()
            implements IcebergColumnKind {}

    record GeneratedRowId(long firstRowId)
            implements IcebergColumnKind {}

    record FileRowId(long firstRowId)
            implements IcebergColumnKind {}

    record FileSequenceNumber(long defaultSequenceNumber)
            implements IcebergColumnKind {}

    record FileBaseColumn()
            implements IcebergColumnKind {}

    record FileProjectedColumn()
            implements IcebergColumnKind {}

    static IcebergColumnKind classify(
            IcebergColumnHandle column,
            Map<Integer, Optional<String>> partitionKeys,
            TrinoInputFile inputFile,
            String partition,
            Schema schema,
            Set<Integer> fileColumnIds,
            OptionalLong fileFirstRowId,
            OptionalLong dataSequenceNumber)
            throws IOException
    {
        verify(!column.isIsDeletedColumn(), "Unexpected is_deleted column");

        if (partitionKeys.containsKey(column.getId())) {
            Type trinoType = column.getType();
            Object value = deserializePartitionValue(trinoType, partitionKeys.get(column.getId()).orElse(null), column.getName());
            return new Constant(column.getType(), value);
        }
        if (column.isPartitionColumn()) {
            return new Constant(PARTITION.getType(), utf8Slice(partition));
        }
        if (column.isPathColumn()) {
            return new Constant(FILE_PATH.getType(), utf8Slice(inputFile.location().toString()));
        }
        if (column.isFileModifiedTimeColumn()) {
            return new Constant(
                    FILE_MODIFIED_TIME.getType(),
                    packDateTimeWithZone(inputFile.lastModified().toEpochMilli(), UTC_KEY));
        }
        if (column.isMergeRowIdColumn()) {
            return new MergeRowId();
        }
        if (column.isRowPositionColumn()) {
            return new RowPosition();
        }
        if (!fileColumnIds.contains(column.getBaseColumnIdentity().getId())) {
            if (column.isRowIdColumn() && fileFirstRowId.isPresent()) {
                return new GeneratedRowId(fileFirstRowId.getAsLong());
            }
            if (column.isLastUpdatedSequenceNumberColumn()) {
                return new Constant(column.getType(), dataSequenceNumber.orElseThrow(() ->
                        new TrinoException(ICEBERG_BAD_DATA, "Cannot read $last_updated_sequence_number metadata column: Iceberg manifest is missing dataSequenceNumber")));
            }
            Object initialDefault = IcebergPageSourceProvider.getInitialDefault(schema, column.getBaseColumnIdentity().getId());
            return new Constant(column.getType(), initialDefault);
        }
        if (column.isRowIdColumn() && fileFirstRowId.isPresent()) {
            return new FileRowId(fileFirstRowId.getAsLong());
        }
        if (column.isLastUpdatedSequenceNumberColumn()) {
            return new FileSequenceNumber(dataSequenceNumber.orElseThrow(() ->
                    new TrinoException(ICEBERG_BAD_DATA, "Cannot read $last_updated_sequence_number metadata column: Iceberg manifest is missing dataSequenceNumber")));
        }
        if (column.isBaseColumn()) {
            return new FileBaseColumn();
        }
        return new FileProjectedColumn();
    }
}
