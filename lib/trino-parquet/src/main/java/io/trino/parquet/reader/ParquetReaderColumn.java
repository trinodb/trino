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
package io.trino.parquet.reader;

import io.trino.parquet.Field;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * @param type Column type
 * @param field Field description. Empty optional will result in column populated with {@code NULL}
 * @param isRowIndexColumn Whether column should be populated with the indices of its rows
 */
public record ParquetReaderColumn(Type type, Optional<Field> field, boolean isRowIndexColumn)
{
    public static List<Field> getParquetReaderFields(List<ParquetReaderColumn> parquetReaderColumns)
    {
        return parquetReaderColumns.stream()
                .filter(column -> !column.isRowIndexColumn())
                .map(ParquetReaderColumn::field)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    public ParquetReaderColumn(Type type, Optional<Field> field, boolean isRowIndexColumn)
    {
        this.type = requireNonNull(type, "type is null");
        this.field = requireNonNull(field, "field is null");
        checkArgument(
                !isRowIndexColumn || field.isEmpty(),
                "Field info for row index column must be empty Optional");
        this.isRowIndexColumn = isRowIndexColumn;
    }
}
