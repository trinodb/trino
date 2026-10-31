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

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.flat.FlatColumnReader;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.reader.ColumnReaderFactory.isSupported;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestColumnReaderFactory
{
    @Test
    public void testTopLevelPrimitiveFields()
    {
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, ParquetReaderOptions.defaultOptions());
        PrimitiveType primitiveType = new PrimitiveType(OPTIONAL, INT32, "test");

        PrimitiveField topLevelRepeatedPrimitiveField = new PrimitiveField(
                INTEGER,
                true,
                new ColumnDescriptor(new String[] {"topLevelRepeatedPrimitiveField test"}, primitiveType, 1, 1),
                0);
        assertThat(columnReaderFactory.create(topLevelRepeatedPrimitiveField, newSimpleAggregatedMemoryContext())).isInstanceOf(NestedColumnReader.class);

        PrimitiveField topLevelOptionalPrimitiveField = new PrimitiveField(
                INTEGER,
                false,
                new ColumnDescriptor(new String[] {"topLevelRequiredPrimitiveField test"}, primitiveType, 0, 1),
                0);
        assertThat(columnReaderFactory.create(topLevelOptionalPrimitiveField, newSimpleAggregatedMemoryContext())).isInstanceOf(FlatColumnReader.class);

        PrimitiveField topLevelRequiredPrimitiveField = new PrimitiveField(
                INTEGER,
                true,
                new ColumnDescriptor(new String[] {"topLevelRequiredPrimitiveField test"}, primitiveType, 0, 0),
                0);
        assertThat(columnReaderFactory.create(topLevelRequiredPrimitiveField, newSimpleAggregatedMemoryContext())).isInstanceOf(FlatColumnReader.class);
    }

    /**
     * A decimal annotated byte array holds a number in its bytes, so handing those bytes back as text or as raw bytes
     * is not a read of the column at all. The two byte array widths have to answer that the same way.
     */
    @Test
    public void testDecimalAnnotatedBinaryIsNotReadAsBytes()
    {
        PrimitiveType decimalBinary = primitiveType(BINARY, decimalType(2, 10));
        for (Type type : ImmutableList.of(VARCHAR, createVarcharType(10), createCharType(10), VARBINARY)) {
            assertThat(isSupported(type, decimalBinary))
                    .describedAs("%s over %s", type, decimalBinary)
                    .isFalse();
        }
    }

    /**
     * {@link ColumnReaderFactory#isSupported} exists so that callers which prune on statistics can ask whether a column
     * is readable without restating the rules of {@link ColumnReaderFactory#create}. A restatement drifts; this walks
     * every combination the two are expected to agree on and fails the moment they stop agreeing.
     */
    @ParameterizedTest
    @MethodSource("readerTypeCombinations")
    public void testIsSupportedAgreesWithCreate(Type type, PrimitiveType parquetType)
    {
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC, ParquetReaderOptions.defaultOptions());
        PrimitiveField field = new PrimitiveField(
                type,
                true,
                new ColumnDescriptor(new String[] {"test"}, parquetType, 0, 0),
                0);

        boolean readerExists;
        try {
            columnReaderFactory.create(field, newSimpleAggregatedMemoryContext());
            readerExists = true;
        }
        catch (TrinoException _) {
            readerExists = false;
        }

        assertThat(isSupported(type, parquetType))
                .describedAs("%s over %s", type, parquetType)
                .isEqualTo(readerExists);
    }

    private static Stream<Arguments> readerTypeCombinations()
    {
        List<Type> trinoTypes = ImmutableList.of(
                BOOLEAN,
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                DATE,
                REAL,
                DOUBLE,
                createDecimalType(9, 0),
                createDecimalType(10, 2),
                createDecimalType(30, 2),
                createVarcharType(10),
                VARCHAR,
                createCharType(10),
                VARBINARY,
                UUID,
                createTimeType(3),
                createTimeType(6),
                createTimestampType(3),
                createTimestampType(9),
                createTimestampWithTimeZoneType(3),
                createTimestampWithTimeZoneType(9));

        List<PrimitiveType> parquetTypes = ImmutableList.of(
                primitiveType(PrimitiveTypeName.BOOLEAN, null),
                primitiveType(INT32, null),
                primitiveType(INT32, intType(32, true)),
                primitiveType(INT32, intType(8, true)),
                primitiveType(INT32, intType(32, false)),
                primitiveType(INT32, decimalType(0, 9)),
                primitiveType(INT32, decimalType(2, 9)),
                primitiveType(INT32, dateType()),
                primitiveType(INT32, timeType(false, MILLIS)),
                primitiveType(INT64, null),
                primitiveType(INT64, intType(64, true)),
                primitiveType(INT64, decimalType(0, 18)),
                primitiveType(INT64, decimalType(2, 18)),
                primitiveType(INT64, timeType(false, MICROS)),
                primitiveType(INT64, timestampType(true, MILLIS)),
                primitiveType(INT64, timestampType(true, MICROS)),
                primitiveType(INT64, timestampType(false, NANOS)),
                primitiveType(INT96, null),
                primitiveType(FLOAT, null),
                primitiveType(PrimitiveTypeName.DOUBLE, null),
                primitiveType(BINARY, null),
                primitiveType(BINARY, stringType()),
                primitiveType(BINARY, decimalType(0, 9)),
                primitiveType(BINARY, decimalType(2, 10)),
                primitiveType(FIXED_LEN_BYTE_ARRAY, null),
                primitiveType(FIXED_LEN_BYTE_ARRAY, decimalType(0, 9)),
                primitiveType(FIXED_LEN_BYTE_ARRAY, decimalType(2, 20)),
                primitiveType(FIXED_LEN_BYTE_ARRAY, uuidType()));

        return trinoTypes.stream()
                .flatMap(type -> parquetTypes.stream().map(parquetType -> Arguments.of(type, parquetType)));
    }

    private static PrimitiveType primitiveType(PrimitiveTypeName typeName, LogicalTypeAnnotation annotation)
    {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.optional(typeName);
        if (typeName == FIXED_LEN_BYTE_ARRAY) {
            builder = builder.length(16);
        }
        return builder.as(annotation).named("test");
    }
}
