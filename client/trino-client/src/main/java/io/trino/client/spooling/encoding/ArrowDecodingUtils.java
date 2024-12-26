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
package io.trino.client.spooling.encoding;

import io.trino.client.ClientTypeSignature;
import io.trino.client.Column;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.UUID;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static io.trino.client.IntervalDayTime.formatMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.nameUUIDFromBytes;

public class ArrowDecodingUtils
{
    // TODO: remove me once I'm not longer needed
    private static final boolean DEBUG = false;

    private static final TypeDecoder PASS_THROUGH_DECODER = new PassThroughDecoder();
    private static final TypeDecoder INTERVAL_DAY_TIME_DECODER = new IntervalDayTimeDecoder();
    private static final TypeDecoder VARCHAR_DECODER = new VarcharDecoder();
    private static final TypeDecoder BIGINT_DECODER = new BigintDecoder();
    private static final TypeDecoder DATE_DECODER = new DateDecoder();
    private static final TypeDecoder DECIMAL_DECODER = new DecimalDecoder();
    private static final TypeDecoder TIMESTAMP_DECODER = new TimestampDecoder();
    private static final TypeDecoder UUID_DECODER = new UuidDecoder();

    private ArrowDecodingUtils()
    {
    }

    public static TypeDecoder[] createTypeDecoders(List<Column> columns)
    {
        verify(!columns.isEmpty(), "Columns must not be empty");
        TypeDecoder[] decoders = new TypeDecoder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            decoders[i] = debugging(createTypeDecoder(columns.get(i).getTypeSignature()));
        }
        return decoders;
    }

    private static TypeDecoder createTypeDecoder(ClientTypeSignature signature)
    {
        switch (signature.getRawType()) {
            case BIGINT:
                return BIGINT_DECODER;
            case INTEGER:
                return PASS_THROUGH_DECODER;
            case SMALLINT:
                return PASS_THROUGH_DECODER;
            case TINYINT:
                return PASS_THROUGH_DECODER;
            case DOUBLE:
                return PASS_THROUGH_DECODER;
            case REAL:
                return PASS_THROUGH_DECODER;
            case BOOLEAN:
                return PASS_THROUGH_DECODER;
            case VARCHAR:
            case CHAR:
                return VARCHAR_DECODER;
            case MAP:
                return new MapDecoder(signature);
            case ARRAY:
                return new ArrayDecoder(signature);
            case TIMESTAMP:
                return TIMESTAMP_DECODER;
            case DATE:
                return DATE_DECODER;
            case UUID:
                return UUID_DECODER;
            case DECIMAL:
                return DECIMAL_DECODER;
            case INTERVAL_DAY_TO_SECOND:
                return INTERVAL_DAY_TIME_DECODER;
//            case ROW:
//            case JSON:
//            case TIME:
//            case TIME_WITH_TIME_ZONE:
//            case TIMESTAMP_WITH_TIME_ZONE:
//            case INTERVAL_YEAR_TO_MONTH:
//            case IPADDRESS:
//            case GEOMETRY:
//            case SPHERICAL_GEOGRAPHY:
//            case COLOR:
//            case KDB_TREE:
//            case BING_TILE:
//            case QDIGEST:
//            case P4_HYPER_LOG_LOG:
//            case HYPER_LOG_LOG:
//            case SET_DIGEST:
//            case VARBINARY:
            default:
                return PASS_THROUGH_DECODER;
        }
    }

    private static class PassThroughDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position);
        }
    }

    private static class VarcharDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof VarCharVector, "Expected vector to be VarCharVector");
            VarCharVector varcharVector = (VarCharVector) vector;
            if (vector.isNull(position)) {
                return null;
            }
            return new String(varcharVector.get(position), UTF_8);
        }
    }

    private static class MapDecoder
            implements TypeDecoder
    {
        private final TypeDecoder keyDecoder;
        private final TypeDecoder valueDecoder;

        public MapDecoder(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyDecoder = debugging(createTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0)));
            this.valueDecoder = debugging(createTypeDecoder(signature.getArgumentsAsTypeSignatures().get(1)));
        }

        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof MapVector, "Expected vector to be MapVector");
            MapVector mapVector = (MapVector) vector;
            StructVector structVector = (StructVector) mapVector.getDataVector();

            FieldVector keyVector = structVector.getChild("key");
            FieldVector fieldVector = structVector.getChild("value");

            Map<Object, Object> values = new HashMap<>();
            for (int i = mapVector.getElementStartIndex(position); i < mapVector.getElementEndIndex(position); i++) {
                values.put(keyDecoder.decode(keyVector, i), valueDecoder.decode(fieldVector, i));
            }
            return unmodifiableMap(values);
        }
    }

    private static class ArrayDecoder
            implements TypeDecoder
    {
        private final TypeDecoder valueDecoder;

        public ArrayDecoder(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(ARRAY), "not an array type signature: %s", signature);
            this.valueDecoder = debugging(createTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0)));
        }

        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof ListVector, "Expected vector to be ListVector");
            ListVector listVector = (ListVector) vector;
            List<Object> values = new ArrayList<>();
            for (int i = listVector.getElementStartIndex(position); i < listVector.getElementEndIndex(position); i++) {
                values.add(valueDecoder.decode(listVector.getDataVector(), i));
            }
            return unmodifiableList(values);
        }
    }

    private static class BigintDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof BigIntVector, "Expected vector to be BigIntVector");
            BigIntVector bigintVector = (BigIntVector) vector;
            return bigintVector.get(position);
        }
    }

    private static class DateDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof DateDayVector, "Expected value to be DateDayVector");
            DateDayVector dateDayVector = (DateDayVector) vector;
            return LocalDate.ofEpochDay(dateDayVector.get(position)).toString();
        }
    }

    private static class TimestampDecoder
            implements TypeDecoder
    {
        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof TimeStampVector, "Expected vector to be TimeStampVector");
            TimeStampVector timeStampVector = (TimeStampVector) vector;
            return formatTimestamp((LocalDateTime) timeStampVector.getObject(position));
        }

        private static String formatTimestamp(LocalDateTime dateTime)
        {
            return TIMESTAMP_FORMATTER.format(dateTime);
            // TODO: fix me
//            if (precision > 0) {
//                long scaledFraction = picoFraction / POWERS_OF_TEN[MAX_PRECISION - precision];
//                builder.append('.');
//                builder.setLength(builder.length() + precision);
//                int index = builder.length() - 1;
//
//                // Append the fractional the decimal digits in reverse order
//                // comparable to format("%0" + precision + "d", scaledFraction);
//                for (int i = 0; i < precision; i++) {
//                    long temp = scaledFraction / 10;
//                    int digit = (int) (scaledFraction - (temp * 10));
//                    scaledFraction = temp;
//                    builder.setCharAt(index - i, (char) ('0' + digit));
//                }
//            }
        }
    }

    private static class DecimalDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof DecimalVector, "Expected vector to be DecimalVector");
            DecimalVector decimalVector = (DecimalVector) vector;
            if (vector.isNull(position)) {
                return null;
            }
            // TODO: expect BigDecimal directly in the JDBC driver
            return decimalVector.getObject(position).toString();
        }
    }

    private static class UuidDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof FixedSizeBinaryVector, "Expected vector to be FixedSizeBinaryVector");
            FixedSizeBinaryVector fixedSizeBinaryVector = (FixedSizeBinaryVector) vector;
            if (fixedSizeBinaryVector.isNull(position)) {
                return null;
            }

            // TODO: expect UUID directly in the JDBC driver
            return nameUUIDFromBytes(fixedSizeBinaryVector.get(position)).toString();
        }
    }

    private static class IntervalDayTimeDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(FieldVector vector, int position)
        {
            verify(vector instanceof IntervalDayVector, "Expected vector to be IntervalDayVector");
            IntervalDayVector intervalDayVector = (IntervalDayVector) vector;
            if (intervalDayVector.isNull(position)) {
                return null;
            }

            Duration duration = intervalDayVector.getObject(position);
            return formatMillis(duration.toNanos() / 1_000_000);
        }
    }

    public interface TypeDecoder
    {
        Object decode(FieldVector vector, int position);
    }

    private static TypeDecoder debugging(TypeDecoder delegate)
    {
        if (!DEBUG) {
            return delegate;
        }
        return (vector, position) -> {
            System.out.println(delegate.getClass().getSimpleName() + "[" + vector.getClass().getName() + "][" + position + "] = " + vector.getObject(position));
            return delegate.decode(vector, position);
        };
    }
}
