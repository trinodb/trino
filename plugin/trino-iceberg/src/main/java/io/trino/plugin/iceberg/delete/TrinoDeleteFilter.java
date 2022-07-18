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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;

import java.util.List;

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
        super(task.file().path().toString(), task.deletes(), tableSchema, toSchema(tableSchema, requestedColumns));
        this.fileIO = requireNonNull(fileIO, "fileIO is null");
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
