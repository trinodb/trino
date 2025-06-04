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

import io.trino.memory.context.LocalMemoryContext;
import io.trino.parquet.PrimitiveField;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.reader.decoders.ValueDecoder.ValueDecodersProvider;
import static io.trino.parquet.reader.decoders.ValueDecoder.createLevelsDecoder;
import static io.trino.parquet.reader.flat.DictionaryDecoder.getDictionaryDecoder;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TestNestedColumnReaderRowRanges
        extends AbstractColumnReaderRowRangesTest
{
    private static final PrimitiveType REQUIRED_TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveType OPTIONAL_TYPE = new PrimitiveType(OPTIONAL, INT32, "");
    /**
     * Quick glossary:
     * -FLAT - field with definition level of 0 (non-null) or 1 (nullable)
     * -NESTED - field with max repetition level
     * -NULLABLE - field with `required` property set to false
     * -REPEATED - field with higher than 0 repetition level
     */
    private static final PrimitiveField FLAT_FIELD = createField(true, 0, 0);
    private static final PrimitiveField NULLABLE_FLAT_FIELD = createField(false, 0, 1);
    private static final PrimitiveField NESTED_FIELD = createField(true, 0, 1);
    private static final PrimitiveField NULLABLE_NESTED_FIELD = createField(false, 0, 2);
    private static final PrimitiveField REPEATED_FLAT_FIELD = createField(true, 1, 0);
    private static final PrimitiveField REPEATED_NESTED_FIELD = createField(true, 1, 1);
    private static final PrimitiveField REPEATED_NULLABLE_FIELD = createField(false, 1, 1);
    private static final PrimitiveField REPEATED_NULLABLE_NESTED_FIELD = createField(false, 1, 2);
    private static final LocalMemoryContext MEMORY_CONTEXT = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");

    private static PrimitiveField createField(boolean required, int maxRep, int maxDef)
    {
        PrimitiveType type = required ? REQUIRED_TYPE : OPTIONAL_TYPE;
        return new PrimitiveField(INTEGER, required, new ColumnDescriptor(new String[] {}, type, maxRep, maxDef), 0);
    }

    @Override
    protected ColumnReaderProvider[] getColumnReaderProviders()
    {
        return NestedColumnReaderProvider.values();
    }

    private enum NestedColumnReaderProvider
            implements ColumnReaderProvider
    {
        NESTED_READER_NO_NULLS(() -> createNestedColumnReader(FLAT_FIELD), FLAT_FIELD),
        NESTED_READER_NULLABLE(() -> createNestedColumnReader(NULLABLE_FLAT_FIELD), NULLABLE_FLAT_FIELD),
        NESTED_READER_NESTED_NO_NULLS(() -> createNestedColumnReader(NESTED_FIELD), NESTED_FIELD),
        NESTED_READER_NESTED_NULLABLE(() -> createNestedColumnReader(NULLABLE_NESTED_FIELD), NULLABLE_NESTED_FIELD),
        NESTED_READER_REPEATABLE_NO_NULLS(() -> createNestedColumnReader(REPEATED_FLAT_FIELD), REPEATED_FLAT_FIELD),
        NESTED_READER_REPEATABLE_NULLABLE(() -> createNestedColumnReader(REPEATED_NULLABLE_FIELD), REPEATED_NULLABLE_FIELD),
        NESTED_READER_REPEATABLE_NESTED_NO_NULLS(() -> createNestedColumnReader(REPEATED_NESTED_FIELD), REPEATED_NESTED_FIELD),
        NESTED_READER_REPEATABLE_NESTED_NULLABLE(() -> createNestedColumnReader(REPEATED_NULLABLE_NESTED_FIELD), REPEATED_NULLABLE_NESTED_FIELD),
        /**/;

        private final Supplier<ColumnReader> columnReader;
        private final PrimitiveField field;

        NestedColumnReaderProvider(Supplier<ColumnReader> columnReader, PrimitiveField field)
        {
            this.columnReader = requireNonNull(columnReader, "columnReader is null");
            this.field = requireNonNull(field, "field is null");
        }

        @Override
        public ColumnReader createColumnReader()
        {
            return columnReader.get();
        }

        @Override
        public PrimitiveField getField()
        {
            return field;
        }
    }

    private static NestedColumnReader<int[]> createNestedColumnReader(PrimitiveField field)
    {
        ValueDecodersProvider<int[]> valueDecodersProvider = getIntDecodersProvider(field);
        return new NestedColumnReader<>(
                field,
                valueDecodersProvider,
                maxLevel -> createLevelsDecoder(maxLevel, false),
                (dictionaryPage, isNonNull) -> getDictionaryDecoder(
                        dictionaryPage,
                        INT_ADAPTER,
                        valueDecodersProvider.create(PLAIN),
                        isNonNull,
                        false),
                INT_ADAPTER,
                MEMORY_CONTEXT);
    }
}
