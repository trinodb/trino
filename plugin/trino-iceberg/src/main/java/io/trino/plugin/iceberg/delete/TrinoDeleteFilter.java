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
package io.trino.plugin.iceberg.delete;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Filter;

import java.util.List;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_UPDATE_ROW_ID_COLUMN_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_UPDATE_ROW_ID_COLUMN_NAME;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.FILE_PATH;
import static org.apache.iceberg.MetadataColumns.IS_DELETED;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;

public class TrinoDeleteFilter
        extends DeleteFilter<TrinoRow>
{
    private final FileIO fileIO;

    public TrinoDeleteFilter(FileScanTask task, Schema tableSchema, List<IcebergColumnHandle> requestedColumns, FileIO fileIO)
    {
        super(task, tableSchema, toSchema(tableSchema, requestedColumns));
        this.fileIO = requireNonNull(fileIO, "fileIO is null");
    }

    /*
     * This does the same as {@link DeleteFilter#filter}. Instead of re-loading and parsing all delete filter
     * files on each invocation they are instead loaded and cache once upon the first call.
     * Hence, filterCached can be used multiple times for small sets or records
     * (such as a Trino {@link io.trino.spi.Page})
     * TODO: Review when https://github.com/apache/iceberg/pull/5195 is released.
     */
    public CloseableIterable<TrinoRow> filterCached(CloseableIterable<TrinoRow> records)
    {
        return filterEqDeletesCached(filterPosDeletesCached(records));
    }

    public CloseableIterable<TrinoRow> filterPosDeletesCached(CloseableIterable<TrinoRow> records)
    {
        if (!hasPosDeletes()) {
            return records;
        }
        // {@link DeleteFilter#deletedRowPositions} loads and caches this filter's positional delete filter
        // files upon the first invocation
        return Deletes.filter(records, this::pos, deletedRowPositions());
    }

    public CloseableIterable<TrinoRow> filterEqDeletesCached(CloseableIterable<TrinoRow> records)
    {
        if (!hasEqDeletes()) {
            return records;
        }
        // {@link DeleteFilter#eqDeletedRowFilter} loads and caches this filter's equality delete filter
        // files upon the first invocation
        Predicate<TrinoRow> remainingRows = eqDeletedRowFilter();

        Filter<TrinoRow> remainingRowsFilter = new Filter<>()
        {
            @Override
            protected boolean shouldKeep(TrinoRow item)
            {
                return remainingRows.test(item);
            }
        };

        return remainingRowsFilter.filter(records);
    }

    @Override
    protected StructLike asStructLike(TrinoRow row)
    {
        return row;
    }

    @Override
    protected InputFile getInputFile(String s)
    {
        return fileIO.newInputFile(s);
    }

    private static Schema toSchema(Schema tableSchema, List<IcebergColumnHandle> requestedColumns)
    {
        return new Schema(requestedColumns.stream().map(column -> toNestedField(tableSchema, column)).collect(toImmutableList()));
    }

    private static Types.NestedField toNestedField(Schema tableSchema, IcebergColumnHandle columnHandle)
    {
        if (columnHandle.isRowPositionColumn()) {
            return ROW_POSITION;
        }
        if (columnHandle.isIsDeletedColumn()) {
            return IS_DELETED;
        }
        if (columnHandle.isPathColumn()) {
            return FILE_PATH;
        }
        if (columnHandle.isUpdateRowIdColumn()) {
            return Types.NestedField.of(TRINO_UPDATE_ROW_ID_COLUMN_ID, false, TRINO_UPDATE_ROW_ID_COLUMN_NAME, Types.StructType.of());
        }

        return tableSchema.findField(columnHandle.getId());
    }
}
