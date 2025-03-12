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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.bigquery.BigQueryTypeManager.toTrinoTimestamp;
import static io.trino.plugin.bigquery.BigQueryUtil.toBigQueryColumnName;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryStorageAvroPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(BigQueryStorageAvroPageSource.class);

    private static final AvroDecimalConverter DECIMAL_CONVERTER = new AvroDecimalConverter();

    private final BigQueryReadClient bigQueryReadClient;
    private final ExecutorService executor;
    private final BigQueryTypeManager typeManager;
    private final String streamName;
    private final Schema avroSchema;
    private final List<BigQueryColumnHandle> columns;
    private final AtomicLong readBytes = new AtomicLong();
    private final AtomicLong readTimeNanos = new AtomicLong();
    private final PageBuilder pageBuilder;
    private final Iterator<ReadRowsResponse> responses;

    private CompletableFuture<ReadRowsResponse> nextResponse;
    private boolean finished;

    public BigQueryStorageAvroPageSource(
            BigQueryReadClient bigQueryReadClient,
            ExecutorService executor,
            BigQueryTypeManager typeManager,
            int maxReadRowsRetries,
            BigQuerySplit split,
            List<BigQueryColumnHandle> columns)
    {
        this.bigQueryReadClient = requireNonNull(bigQueryReadClient, "bigQueryReadClient is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        requireNonNull(split, "split is null");
        this.streamName = split.streamName();
        this.avroSchema = parseSchema(split.schemaString());
        this.columns = requireNonNull(columns, "columns is null");
        this.pageBuilder = new PageBuilder(columns.stream()
                .map(BigQueryColumnHandle::trinoType)
                .collect(toImmutableList()));

        log.debug("Starting to read from %s", streamName);
        responses = new ReadRowsHelper(bigQueryReadClient, streamName, maxReadRowsRetries).readRows();
        nextResponse = CompletableFuture.supplyAsync(this::getResponse, executor);
    }

    private Schema parseSchema(String schemaString)
    {
        try {
            return new Schema.Parser().parse(schemaString);
        }
        catch (SchemaParseException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid Avro schema: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        ReadRowsResponse response;
        try {
            response = getFutureValue(nextResponse);
        }
        catch (NoSuchElementException ignored) {
            finished = true;
            return null;
        }
        nextResponse = CompletableFuture.supplyAsync(this::getResponse, executor);
        long start = System.nanoTime();
        Iterable<GenericRecord> records = parse(response);
        for (GenericRecord record : records) {
            pageBuilder.declarePosition();
            for (int column = 0; column < columns.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                BigQueryColumnHandle columnHandle = columns.get(column);
                appendTo(columnHandle.trinoType(), getValueRecord(record, columnHandle), output);
            }
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        readTimeNanos.addAndGet(System.nanoTime() - start);
        return SourcePage.create(page);
    }

    private static Object getValueRecord(GenericRecord record, BigQueryColumnHandle columnHandle)
    {
        Object valueRecord = record.get(toBigQueryColumnName(columnHandle.name()));
        for (String dereferenceName : columnHandle.dereferenceNames()) {
            if (valueRecord == null) {
                break;
            }
            if (valueRecord instanceof GenericRecord genericRecord) {
                valueRecord = genericRecord.get(dereferenceName);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to extract dereference value from record");
            }
        }
        return valueRecord;
    }

    private void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type instanceof DecimalType decimalType) {
                    verify(decimalType.isShort(), "The type should be short decimal");
                    BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
                    type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    type.writeLong(output, toTrinoTimestamp(((Utf8) value).toString()));
                }
                else if (type.equals(TIME_MICROS)) {
                    type.writeLong(output, (long) value * PICOSECONDS_PER_MICROSECOND);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (type.getJavaType() == Int128.class) {
                writeObject(output, type, value);
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                verify(type.equals(TIMESTAMP_TZ_MICROS));
                long epochMicros = (long) value;
                int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
                type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
            }
            else if (type instanceof ArrayType arrayType) {
                writeArray((ArrayBlockBuilder) output, (List<?>) value, arrayType);
            }
            else if (type instanceof RowType rowType) {
                writeRow((RowBlockBuilder) output, rowType, (GenericRecord) value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                type.writeSlice(output, Slices.wrappedHeapBuffer((ByteBuffer) value));
            }
            else {
                output.appendNull();
            }
        }
        else if (typeManager.isJsonType(type)) {
            type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            verify(!decimalType.isShort(), "The type should be long decimal");
            BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeArray(ArrayBlockBuilder output, List<?> value, ArrayType arrayType)
    {
        Type elementType = arrayType.getElementType();
        output.buildEntry(elementBuilder -> {
            for (Object element : value) {
                appendTo(elementType, element, elementBuilder);
            }
        });
    }

    private void writeRow(RowBlockBuilder output, RowType rowType, GenericRecord record)
    {
        List<Field> fields = rowType.getFields();
        output.buildEntry(fieldBuilders -> {
            for (int index = 0; index < fields.size(); index++) {
                Field field = fields.get(index);
                appendTo(field.getType(), record.get(field.getName().orElse("field" + index)), fieldBuilders.get(index));
            }
        });
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
        nextResponse.cancel(true);
        bigQueryReadClient.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return nextResponse;
    }

    private ReadRowsResponse getResponse()
    {
        long start = System.nanoTime();
        ReadRowsResponse response = responses.next();
        readTimeNanos.addAndGet(System.nanoTime() - start);
        return response;
    }

    Iterable<GenericRecord> parse(ReadRowsResponse response)
    {
        byte[] buffer = response.getAvroRows().getSerializedBinaryRows().toByteArray();
        readBytes.addAndGet(buffer.length);
        log.debug("Read %d bytes (total %d) from %s", buffer.length, readBytes.get(), streamName);
        return () -> new AvroBinaryIterator(avroSchema, buffer);
    }

    private static class AvroBinaryIterator
            implements Iterator<GenericRecord>
    {
        GenericDatumReader<GenericRecord> reader;
        BinaryDecoder in;

        AvroBinaryIterator(Schema avroSchema, byte[] buffer)
        {
            this.reader = new GenericDatumReader<>(avroSchema);
            this.in = new DecoderFactory().binaryDecoder(buffer, null);
        }

        @Override
        public boolean hasNext()
        {
            try {
                return !in.isEnd();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error determining the end of Avro buffer", e);
            }
        }

        @Override
        public GenericRecord next()
        {
            try {
                return reader.read(null, in);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Error reading next Avro Record", e);
            }
        }
    }

    static class AvroDecimalConverter
    {
        private static final DecimalConversion AVRO_DECIMAL_CONVERSION = new DecimalConversion();

        BigDecimal convert(int precision, int scale, Object value)
        {
            Schema schema = new Schema.Parser().parse(format("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}", precision, scale));
            return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer) value, schema, schema.getLogicalType());
        }
    }
}
