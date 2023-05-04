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
package org.apache.iceberg;

import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iceberg.MetricsUtil.READABLE_METRICS;

public class FilesMetadataTable
        extends FilesTable
        implements TrinoMetadataTable
{
    public static final int CONTENT = 134; //DataFile.CONTENT.fieldId();
    public static final int FILE_PATH = 100; //DataFile.FILE_PATH.fieldId();
    public static final int FILE_FORMAT = 101; //DataFile.FILE_FORMAT.fieldId();
    public static final int RECORD_COUNT = 103; //DataFile.RECORD_COUNT.fieldId();
    public static final int FILE_SIZE = 104; //DataFile.FILE_SIZE.fieldId();
    public static final int COLUMN_SIZES = 108; //DataFile.COLUMN_SIZES.fieldId();
    public static final int VALUE_COUNTS = 109; //DataFile.VALUE_COUNTS.fieldId();
    public static final int NULL_VALUE_COUNTS = 110; //DataFile.NULL_VALUE_COUNTS.fieldId();
    public static final int NAN_VALUE_COUNTS = 137; //DataFile.NAN_VALUE_COUNTS.fieldId();
    public static final int LOWER_BOUNDS = 125; //DataFile.LOWER_BOUNDS.fieldId();
    public static final int UPPER_BOUNDS = 128; //DataFile.UPPER_BOUNDS.fieldId();
    public static final int KEY_METADATA = 131; //DataFile.KEY_METADATA.fieldId();
    public static final int SPLIT_OFFSETS = 132; //DataFile.SPLIT_OFFSETS.fieldId();
    public static final int EQUALITY_IDS = 135; //DataFile.EQUALITY_IDS.fieldId();
    public static final int SORT_ORDER_ID = 140; //DataFile.SORT_ORDER_ID.fieldId();
    public static final int SPEC_ID = 141; //DataFile.SPEC_ID.fieldId();
    public static final int PARTITION_ID = DataFile.PARTITION_ID;
    public static final int HIDDEN_FILE_PATH = Integer.MAX_VALUE - 1;
    public static final int HIDDEN_MODIFIED_FILE_PATH = Integer.MAX_VALUE - 1001;
    private final Schema schema;

    public FilesMetadataTable(Table table)
    {
        super(table);
        // Transform the base Files table schema so lower bounds and upper bounds are readable human strings instead of byte buffers
        Schema schema = super.schema();
        List<Types.NestedField> columns = schema.columns()
                .stream()
                .filter(column -> !column.name().equals(READABLE_METRICS))
                .map(column -> {
                    if (column.fieldId() == LOWER_BOUNDS) {
                        return Types.NestedField.of(column.fieldId(), column.isOptional(), column.name(), Types.MapType.ofRequired(126, 127, Types.StringType.get(), Types.StringType.get()));
                    }
                    else if (column.fieldId() == UPPER_BOUNDS) {
                        return Types.NestedField.of(column.fieldId(), column.isOptional(), column.name(), Types.MapType.ofRequired(129, 130, Types.StringType.get(), Types.StringType.get()));
                    }
                    else {
                        return column;
                    }
                })
                .collect(Collectors.toList());
        this.schema = new Schema(columns);
    }

    @Override
    public Schema schema()
    {
        return this.schema;
    }

    @Override
    public Table baseTable()
    {
        return super.table();
    }
}
