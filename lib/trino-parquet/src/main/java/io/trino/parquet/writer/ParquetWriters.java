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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.writer.valuewriter.BigintValueWriter;
import io.trino.parquet.writer.valuewriter.BinaryValueWriter;
import io.trino.parquet.writer.valuewriter.BooleanValueWriter;
import io.trino.parquet.writer.valuewriter.DateValueWriter;
import io.trino.parquet.writer.valuewriter.DoubleValueWriter;
import io.trino.parquet.writer.valuewriter.FixedLenByteArrayLongDecimalValueWriter;
import io.trino.parquet.writer.valuewriter.FixedLenByteArrayShortDecimalValueWriter;
import io.trino.parquet.writer.valuewriter.Int32ShortDecimalValueWriter;
import io.trino.parquet.writer.valuewriter.Int64ShortDecimalValueWriter;
import io.trino.parquet.writer.valuewriter.Int96TimestampValueWriter;
import io.trino.parquet.writer.valuewriter.IntegerValueWriter;
import io.trino.parquet.writer.valuewriter.PrimitiveValueWriter;
import io.trino.parquet.writer.valuewriter.RealValueWriter;
import io.trino.parquet.writer.valuewriter.TimeMicrosValueWriter;
import io.trino.parquet.writer.valuewriter.TimestampMillisValueWriter;
import io.trino.parquet.writer.valuewriter.TimestampNanosValueWriter;
import io.trino.parquet.writer.valuewriter.TimestampTzMicrosValueWriter;
import io.trino.parquet.writer.valuewriter.TimestampTzMillisValueWriter;
import io.trino.parquet.writer.valuewriter.TrinoValuesWriterFactory;
import io.trino.parquet.writer.valuewriter.UuidValueWriter;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.joda.time.DateTimeZone;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

final class ParquetWriters
{
    private ParquetWriters() {}

    static List<ColumnWriter> getColumnWriters(
            MessageType messageType,
            Map<List<String>, Type> trinoTypes,
            ParquetProperties parquetProperties,
            CompressionCodec compressionCodec,
            Optional<DateTimeZone> parquetTimeZone)
    {
        TrinoValuesWriterFactory valuesWriterFactory = new TrinoValuesWriterFactory(parquetProperties);
        WriteBuilder writeBuilder = new WriteBuilder(messageType, trinoTypes, parquetProperties, valuesWriterFactory, compressionCodec, parquetTimeZone);
        ParquetTypeVisitor.visit(messageType, writeBuilder);
        return writeBuilder.build();
    }

    private static class WriteBuilder
            extends ParquetTypeVisitor<ColumnWriter>
    {
        private final MessageType type;
        private final Map<List<String>, Type> trinoTypes;
        private final ParquetProperties parquetProperties;
        private final TrinoValuesWriterFactory valuesWriterFactory;
        private final CompressionCodec compressionCodec;
        private final Optional<DateTimeZone> parquetTimeZone;
        private final ImmutableList.Builder<ColumnWriter> builder = ImmutableList.builder();

        WriteBuilder(
                MessageType messageType,
                Map<List<String>, Type> trinoTypes,
                ParquetProperties parquetProperties,
                TrinoValuesWriterFactory valuesWriterFactory,
                CompressionCodec compressionCodec,
                Optional<DateTimeZone> parquetTimeZone)
        {
            this.type = requireNonNull(messageType, "messageType is null");
            this.trinoTypes = requireNonNull(trinoTypes, "trinoTypes is null");
            this.parquetProperties = requireNonNull(parquetProperties, "parquetProperties is null");
            this.valuesWriterFactory = requireNonNull(valuesWriterFactory, "valuesWriterFactory is null");
            this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
            this.parquetTimeZone = requireNonNull(parquetTimeZone, "parquetTimeZone is null");
        }

        List<ColumnWriter> build()
        {
            return builder.build();
        }

        @Override
        public ColumnWriter message(MessageType message, List<ColumnWriter> fields)
        {
            builder.addAll(fields);
            return super.message(message, fields);
        }

        @Override
        public ColumnWriter struct(GroupType struct, List<ColumnWriter> fields)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            return new StructColumnWriter(ImmutableList.copyOf(fields), fieldDefinitionLevel);
        }

        @Override
        public ColumnWriter list(GroupType array, ColumnWriter element)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            return new ArrayColumnWriter(element, fieldDefinitionLevel, fieldRepetitionLevel);
        }

        @Override
        public ColumnWriter map(GroupType map, ColumnWriter key, ColumnWriter value)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            return new MapColumnWriter(key, value, fieldDefinitionLevel, fieldRepetitionLevel);
        }

        @Override
        public ColumnWriter primitive(PrimitiveType primitive)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            ColumnDescriptor columnDescriptor = new ColumnDescriptor(path, primitive, fieldRepetitionLevel, fieldDefinitionLevel);
            Type trinoType = requireNonNull(trinoTypes.get(ImmutableList.copyOf(path)), "Trino type is null");
            return new PrimitiveColumnWriter(
                    columnDescriptor,
                    getValueWriter(valuesWriterFactory.newValuesWriter(columnDescriptor), trinoType, columnDescriptor.getPrimitiveType(), parquetTimeZone),
                    parquetProperties.newDefinitionLevelWriter(columnDescriptor),
                    parquetProperties.newRepetitionLevelWriter(columnDescriptor),
                    compressionCodec,
                    parquetProperties.getPageSizeThreshold());
        }

        private String[] currentPath()
        {
            String[] path = new String[fieldNames.size()];
            if (!fieldNames.isEmpty()) {
                Iterator<String> iter = fieldNames.descendingIterator();
                for (int i = 0; iter.hasNext(); i += 1) {
                    path[i] = iter.next();
                }
            }
            return path;
        }
    }

    private static PrimitiveValueWriter getValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType, Optional<DateTimeZone> parquetTimeZone)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanValueWriter(valuesWriter, parquetType);
        }
        if (INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)) {
            return new IntegerValueWriter(valuesWriter, type, parquetType);
        }
        if (BIGINT.equals(type)) {
            return new BigintValueWriter(valuesWriter, type, parquetType);
        }
        if (type instanceof DecimalType) {
            if (parquetType.getPrimitiveTypeName() == INT32) {
                return new Int32ShortDecimalValueWriter(valuesWriter, type, parquetType);
            }
            if (parquetType.getPrimitiveTypeName() == INT64) {
                return new Int64ShortDecimalValueWriter(valuesWriter, type, parquetType);
            }
            if (((DecimalType) type).isShort()) {
                return new FixedLenByteArrayShortDecimalValueWriter(valuesWriter, type, parquetType);
            }
            return new FixedLenByteArrayLongDecimalValueWriter(valuesWriter, type, parquetType);
        }
        if (DATE.equals(type)) {
            return new DateValueWriter(valuesWriter, parquetType);
        }
        if (TIME_MICROS.equals(type)) {
            verifyParquetType(type, parquetType, TimeLogicalTypeAnnotation.class, isTime(LogicalTypeAnnotation.TimeUnit.MICROS));
            return new TimeMicrosValueWriter(valuesWriter, parquetType);
        }
        if (type instanceof TimestampType) {
            if (parquetType.getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT96)) {
                checkArgument(parquetTimeZone.isPresent(), "parquetTimeZone must be provided for INT96 timestamps");
                return new Int96TimestampValueWriter(valuesWriter, type, parquetType, parquetTimeZone.get());
            }
            if (TIMESTAMP_MILLIS.equals(type)) {
                verifyParquetType(type, parquetType, TimestampLogicalTypeAnnotation.class, isTimestamp(LogicalTypeAnnotation.TimeUnit.MILLIS));
                return new TimestampMillisValueWriter(valuesWriter, type, parquetType);
            }
            if (TIMESTAMP_MICROS.equals(type)) {
                verifyParquetType(type, parquetType, TimestampLogicalTypeAnnotation.class, isTimestamp(LogicalTypeAnnotation.TimeUnit.MICROS));
                return new BigintValueWriter(valuesWriter, type, parquetType);
            }
            if (TIMESTAMP_NANOS.equals(type)) {
                verifyParquetType(type, parquetType, TimestampLogicalTypeAnnotation.class, isTimestamp(LogicalTypeAnnotation.TimeUnit.NANOS));
                return new TimestampNanosValueWriter(valuesWriter, type, parquetType);
            }
        }

        if (TIMESTAMP_TZ_MILLIS.equals(type)) {
            return new TimestampTzMillisValueWriter(valuesWriter, parquetType);
        }
        if (TIMESTAMP_TZ_MICROS.equals(type)) {
            return new TimestampTzMicrosValueWriter(valuesWriter, parquetType);
        }
        if (DOUBLE.equals(type)) {
            return new DoubleValueWriter(valuesWriter, parquetType);
        }
        if (REAL.equals(type)) {
            return new RealValueWriter(valuesWriter, parquetType);
        }
        if (type instanceof VarcharType || type instanceof CharType || type instanceof VarbinaryType) {
            // Binary writer is suitable also for char data, as UTF-8 encoding is used on both sides.
            return new BinaryValueWriter(valuesWriter, type, parquetType);
        }
        if (type instanceof UuidType) {
            return new UuidValueWriter(valuesWriter, parquetType);
        }
        throw new TrinoException(NOT_SUPPORTED, format("Unsupported type for Parquet writer: %s", type));
    }

    private static <T> void verifyParquetType(Type type, PrimitiveType parquetType, Class<T> annotationType, Predicate<T> predicate)
    {
        checkArgument(
                annotationType.isInstance(parquetType.getLogicalTypeAnnotation()) &&
                        predicate.test(annotationType.cast(parquetType.getLogicalTypeAnnotation())),
                "Wrong Parquet type '%s' for Trino type '%s'", parquetType, type);
    }

    private static Predicate<TimeLogicalTypeAnnotation> isTime(LogicalTypeAnnotation.TimeUnit precision)
    {
        requireNonNull(precision, "precision is null");
        return annotation -> annotation.getUnit() == precision &&
                // isAdjustedToUTC=false indicates Local semantics (timestamps not normalized to UTC)
                !annotation.isAdjustedToUTC();
    }

    private static Predicate<TimestampLogicalTypeAnnotation> isTimestamp(LogicalTypeAnnotation.TimeUnit precision)
    {
        requireNonNull(precision, "precision is null");
        return annotation -> annotation.getUnit() == precision &&
                // isAdjustedToUTC=false indicates Local semantics (timestamps not normalized to UTC)
                !annotation.isAdjustedToUTC();
    }
}
