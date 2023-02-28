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

import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.flat.FlatColumnReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestColumnReaderFactory
{
    @Test
    public void testUseBatchedColumnReaders()
    {
        PrimitiveField field = new PrimitiveField(
                INTEGER,
                false,
                new ColumnDescriptor(new String[] {"test"}, new PrimitiveType(OPTIONAL, INT32, "test"), 0, 1),
                0);
        assertThat(ColumnReaderFactory.create(field, UTC, newSimpleAggregatedMemoryContext(), new ParquetReaderOptions().withBatchColumnReaders(false)))
                .isNotInstanceOf(AbstractColumnReader.class);
        assertThat(ColumnReaderFactory.create(field, UTC, newSimpleAggregatedMemoryContext(), new ParquetReaderOptions().withBatchColumnReaders(true)))
                .isInstanceOf(FlatColumnReader.class);
    }

    @Test
    public void testNestedColumnReaders()
    {
        PrimitiveField field = new PrimitiveField(
                INTEGER,
                false,
                new ColumnDescriptor(new String[] {"level1", "level2"}, new PrimitiveType(OPTIONAL, INT32, "test"), 1, 2),
                0);
        assertThat(ColumnReaderFactory.create(field, UTC, newSimpleAggregatedMemoryContext(), new ParquetReaderOptions().withBatchColumnReaders(false)))
                .isNotInstanceOf(AbstractColumnReader.class);
        assertThat(ColumnReaderFactory.create(
                field,
                UTC,
                newSimpleAggregatedMemoryContext(),
                new ParquetReaderOptions().withBatchColumnReaders(false).withBatchNestedColumnReaders(true)))
                .isNotInstanceOf(AbstractColumnReader.class);

        assertThat(ColumnReaderFactory.create(field, UTC, newSimpleAggregatedMemoryContext(), new ParquetReaderOptions().withBatchColumnReaders(true)))
                .isInstanceOf(NestedColumnReader.class);
        assertThat(ColumnReaderFactory.create(
                field,
                UTC,
                newSimpleAggregatedMemoryContext(),
                new ParquetReaderOptions().withBatchColumnReaders(true).withBatchNestedColumnReaders(true)))
                .isInstanceOf(NestedColumnReader.class);
    }
}
