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
import io.trino.parquet.reader.decoders.ValueDecoders;
import io.trino.parquet.reader.flat.FlatColumnReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;

import java.util.function.Supplier;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.reader.flat.IntColumnAdapter.INT_ADAPTER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TestFlatColumnReaderRowRanges
        extends AbstractColumnReaderRowRangesTest
{
    private static final PrimitiveType REQUIRED_TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveType OPTIONAL_TYPE = new PrimitiveType(OPTIONAL, INT32, "");
    /**
     * Quick glossary:
     * -FLAT - field with definition level of 0 (non-null) or 1 (nullable)
     * -NULLABLE - field with `required` property set to false
     */
    private static final PrimitiveField NULLABLE_FIELD = createField(false, 0, 1);
    private static final PrimitiveField FIELD = createField(true, 0, 0);
    private static final LocalMemoryContext MEMORY_CONTEXT = newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");

    private static PrimitiveField createField(boolean required, int maxRep, int maxDef)
    {
        PrimitiveType type = required ? REQUIRED_TYPE : OPTIONAL_TYPE;
        return new PrimitiveField(INTEGER, required, new ColumnDescriptor(new String[] {}, type, maxRep, maxDef), 0);
    }

    @Override
    protected ColumnReaderProvider[] getColumnReaderProviders()
    {
        return FlatColumnReaderProvider.values();
    }

    private enum FlatColumnReaderProvider
            implements ColumnReaderProvider
    {
        INT_PRIMITIVE_NO_NULLS(() -> new IntColumnReader(FIELD), FIELD),
        INT_PRIMITIVE_NULLABLE(() -> new IntColumnReader(NULLABLE_FIELD), NULLABLE_FIELD),
        INT_FLAT_NO_NULLS(() -> new FlatColumnReader<>(FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER, MEMORY_CONTEXT), FIELD),
        INT_FLAT_NULLABLE(() -> new FlatColumnReader<>(NULLABLE_FIELD, ValueDecoders::getIntDecoder, INT_ADAPTER, MEMORY_CONTEXT), NULLABLE_FIELD),
        /**/;

        private final Supplier<ColumnReader> columnReader;
        private final PrimitiveField field;

        FlatColumnReaderProvider(Supplier<ColumnReader> columnReader, PrimitiveField field)
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
}
