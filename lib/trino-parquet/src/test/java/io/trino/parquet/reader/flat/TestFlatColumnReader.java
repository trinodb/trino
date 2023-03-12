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
package io.trino.parquet.reader.flat;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.parquet.DataPage;
import io.trino.parquet.DataPageV1;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.AbstractColumnReaderTest;
import io.trino.parquet.reader.ColumnReader;
import io.trino.parquet.reader.ColumnReaderFactory;
import io.trino.parquet.reader.PageReader;
import io.trino.spi.block.Block;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetEncoding.BIT_PACKED;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.reader.TestingColumnReader.PLAIN_WRITER;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

/**
 * The purpose of this class is to cover all code paths of FlatColumnReader with all supported data types.
 */
public class TestFlatColumnReader
        extends AbstractColumnReaderTest
{
    private static final PrimitiveType TYPE = new PrimitiveType(REQUIRED, INT32, "");
    private static final PrimitiveField NULLABLE_FIELD = new PrimitiveField(INTEGER, false, new ColumnDescriptor(new String[] {"test"}, TYPE, 0, 0), 0);
    private static final PrimitiveField FIELD = new PrimitiveField(INTEGER, true, new ColumnDescriptor(new String[] {"test"}, TYPE, 0, 0), 0);

    @Override
    protected ColumnReader createColumnReader(PrimitiveField field)
    {
        ColumnReader columnReader = ColumnReaderFactory.create(
                field,
                UTC,
                newSimpleAggregatedMemoryContext(),
                new ParquetReaderOptions().withBatchColumnReaders(true));
        assertThat(columnReader).isInstanceOf(FlatColumnReader.class);
        return columnReader;
    }

    @Test
    public void testReadPageV1BitPacked()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(NULLABLE_FIELD);
        reader.setPageReader(getSimplePageReaderMock(BIT_PACKED), Optional.empty());
        reader.prepareNextRead(1);

        assertThatThrownBy(reader::readNullable)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid definition level encoding: BIT_PACKED");
    }

    @Test
    public void testReadPageV1BitPackedNoNulls()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        reader.setPageReader(getSimplePageReaderMock(BIT_PACKED), Optional.empty());
        reader.prepareNextRead(1);

        reader.readNonNull();
    }

    @Test
    public void testReadPageV1RleNoNulls()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(FIELD);
        assertThat(FIELD.isRequired()).isTrue();
        assertThat(FIELD.getDescriptor().getMaxDefinitionLevel()).isEqualTo(0);
        reader.setPageReader(getSimplePageReaderMock(RLE), Optional.empty());
        reader.prepareNextRead(1);

        Block block = reader.readNonNull().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(block.getInt(0, 0)).isEqualTo(42);
    }

    @Test
    public void testReadPageV1RleOnlyNulls()
            throws IOException
    {
        FlatColumnReader<?> reader = (FlatColumnReader<?>) createColumnReader(NULLABLE_FIELD);
        assertThat(NULLABLE_FIELD.isRequired()).isFalse();
        assertThat(NULLABLE_FIELD.getDescriptor().getMaxDefinitionLevel()).isEqualTo(0);
        reader.setPageReader(getNullOnlyPageReaderMock(), Optional.empty());
        reader.prepareNextRead(1);

        Block block = reader.readNullable().getBlock();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(block.isNull(0)).isTrue();
    }

    private static PageReader getSimplePageReaderMock(ParquetEncoding encoding)
            throws IOException
    {
        ValuesWriter writer = PLAIN_WRITER.apply(1);
        writer.writeInteger(42);
        byte[] valueBytes = writer.getBytes().toByteArray();
        List<DataPage> pages = ImmutableList.of(new DataPageV1(
                Slices.wrappedBuffer(valueBytes),
                1,
                valueBytes.length,
                OptionalLong.empty(),
                encoding,
                encoding,
                PLAIN));
        return new PageReader(UNCOMPRESSED, pages.iterator(), false, false);
    }

    private static PageReader getNullOnlyPageReaderMock()
            throws IOException
    {
        RunLengthBitPackingHybridValuesWriter nullsWriter = new RunLengthBitPackingHybridValuesWriter(1, 1, 1, HeapByteBufferAllocator.getInstance());
        nullsWriter.writeInteger(0);
        byte[] nullBytes = nullsWriter.getBytes().toByteArray();
        List<DataPage> pages = ImmutableList.of(new DataPageV1(
                Slices.wrappedBuffer(nullBytes),
                1,
                nullBytes.length,
                OptionalLong.empty(),
                RLE,
                RLE,
                PLAIN));
        return new PageReader(UNCOMPRESSED, pages.iterator(), false, false);
    }
}
