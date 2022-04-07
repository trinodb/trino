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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class TrinoDeleteFilter
        extends DeleteFilter<TrinoRow>
{
    private final FileIO fileIO;

    public TrinoDeleteFilter(FileScanTask task, Schema tableSchema, List<IcebergColumnHandle> requestedColumns, FileIO fileIO)
    {
        super(task, tableSchema, filterSchema(tableSchema, requestedColumns));
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

    private static Schema filterSchema(Schema tableSchema, List<IcebergColumnHandle> requestedColumns)
    {
        Set<Integer> requestedFieldIds = requestedColumns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        return new Schema(filterFieldList(tableSchema.columns(), requestedFieldIds));
    }

    private static List<Types.NestedField> filterFieldList(List<Types.NestedField> fields, Set<Integer> requestedFieldIds)
    {
        return fields.stream()
                .map(field -> filterField(field, requestedFieldIds))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    private static Optional<Types.NestedField> filterField(Types.NestedField field, Set<Integer> requestedFieldIds)
    {
        Type fieldType = field.type();
        if (requestedFieldIds.contains(field.fieldId())) {
            return Optional.of(field);
        }

        if (fieldType.isStructType()) {
            List<Types.NestedField> requiredChildren = filterFieldList(fieldType.asStructType().fields(), requestedFieldIds);
            if (requiredChildren.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(Types.NestedField.of(
                    field.fieldId(),
                    field.isOptional(),
                    field.name(),
                    Types.StructType.of(requiredChildren),
                    field.doc()));
        }

        return Optional.empty();
    }
}
