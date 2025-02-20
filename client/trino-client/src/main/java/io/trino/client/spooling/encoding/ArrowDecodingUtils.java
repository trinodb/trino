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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.TransferPair;

import java.io.Closeable;
import java.io.IOException;
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
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.nameUUIDFromBytes;

public class ArrowDecodingUtils
{
    // TODO: remove me once I'm not longer needed
    private static final boolean DEBUG = false;

    private ArrowDecodingUtils()
    {
    }

    public static VectorTypeDecoder[] createVectorTypeDecoders(List<Column> columns, BufferAllocator allocator, List<FieldVector> vectors)
    {
        verify(!columns.isEmpty(), "Columns must not be empty");
        VectorTypeDecoder[] decoders = new VectorTypeDecoder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            TransferPair transferPair = vectors.get(i).getTransferPair(allocator);
            transferPair.getTo().allocateNew();
            transferPair.transfer();
            decoders[i] = debugging(createVectorTypeDecoder(columns.get(i).getTypeSignature(), transferPair.getTo()));
        }
        return decoders;
    }

    private static VectorTypeDecoder createVectorTypeDecoder(ClientTypeSignature signature, ValueVector vector)
    {
        switch (signature.getRawType()) {
            case BIGINT:
                return new BigintDecoder(checkedCast(vector, BigIntVector.class));
            case INTEGER:
                return new PassThroughDecoder(vector);
            case SMALLINT:
                return new PassThroughDecoder(vector);
            case TINYINT:
                return new PassThroughDecoder(vector);
            case DOUBLE:
                return new PassThroughDecoder(vector);
            case REAL:
                return new PassThroughDecoder(vector);
            case BOOLEAN:
                return new PassThroughDecoder(vector);
            case VARCHAR:
            case CHAR:
                return new VarcharDecoder(checkedCast(vector, VarCharVector.class));
            case MAP:
                return new MapDecoder(signature, checkedCast(vector, MapVector.class));
            case ARRAY:
                return new ArrayDecoder(signature, checkedCast(vector, ListVector.class));
            case TIMESTAMP:
                return new TimestampDecoder(checkedCast(vector, TimeStampVector.class));
            case DATE:
                return new DateDecoder(checkedCast(vector, DateDayVector.class));
            case UUID:
                return new UuidDecoder(checkedCast(vector, FixedSizeBinaryVector.class));
            case DECIMAL:
                return new DecimalDecoder(checkedCast(vector, DecimalVector.class));
            case INTERVAL_DAY_TO_SECOND:
                return new IntervalDayTimeDecoder(checkedCast(vector, IntervalDayVector.class));
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
                return new PassThroughDecoder(vector);
        }
    }

    private static class PassThroughDecoder
            implements VectorTypeDecoder
    {
        private final ValueVector vector;

        public PassThroughDecoder(ValueVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class VarcharDecoder
            implements VectorTypeDecoder
    {
        private final VarCharVector vector;

        public VarcharDecoder(VarCharVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class MapDecoder
            implements VectorTypeDecoder
    {
        private final VectorTypeDecoder keyDecoder;
        private final VectorTypeDecoder valueDecoder;
        private final MapVector vector;

        public MapDecoder(ClientTypeSignature signature, MapVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");
            StructVector structVector = (StructVector) vector.getDataVector();

            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyDecoder = debugging(createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0), structVector.getChild("key")));
            this.valueDecoder = debugging(createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(1), structVector.getChild("value")));
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            if (vector.isEmpty(position)) {
                return emptyMap();
            }

            Map<Object, Object> values = new HashMap<>();
            for (int i = vector.getElementStartIndex(position); i < vector.getElementEndIndex(position); i++) {
                values.put(keyDecoder.decode(i), valueDecoder.decode(i));
            }
            return unmodifiableMap(values);
        }

        @Override
        public void close()
                throws IOException
        {
            keyDecoder.close();
            valueDecoder.close();
            vector.close();
        }
    }

    private static class ArrayDecoder
            implements VectorTypeDecoder
    {
        private final VectorTypeDecoder valueDecoder;
        private final ListVector vector;

        public ArrayDecoder(ClientTypeSignature signature, ListVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");
            checkArgument(signature.getRawType().equals(ARRAY), "not an array type signature: %s", signature);
            this.valueDecoder = debugging(createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0), vector.getDataVector()));
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            if (vector.isEmpty(position)) {
                return emptyList();
            }

            List<Object> values = new ArrayList<>();
            for (int i = vector.getElementStartIndex(position); i < vector.getElementEndIndex(position); i++) {
                values.add(valueDecoder.decode(i));
            }
            return unmodifiableList(values);
        }

        @Override
        public void close()
                throws IOException
        {
            valueDecoder.close();
            vector.close();
        }
    }

    private static class BigintDecoder
            implements VectorTypeDecoder
    {
        private final BigIntVector vector;

        public BigintDecoder(BigIntVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.get(position);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class DateDecoder
            implements VectorTypeDecoder
    {
        private final DateDayVector vector;

        public DateDecoder(DateDayVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return LocalDate.ofEpochDay(vector.get(position)).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampDecoder
            implements VectorTypeDecoder
    {
        private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");

        private final TimeStampVector vector;

        public TimestampDecoder(TimeStampVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatTimestamp((LocalDateTime) vector.getObject(position));
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

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class DecimalDecoder
            implements VectorTypeDecoder
    {
        private final DecimalVector vector;

        public DecimalDecoder(DecimalVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            // TODO: expect BigDecimal directly in the JDBC driver
            return vector.getObject(position).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class UuidDecoder
            implements VectorTypeDecoder
    {
        private final FixedSizeBinaryVector vector;

        public UuidDecoder(FixedSizeBinaryVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            // TODO: expect UUID directly in the JDBC driver
            return nameUUIDFromBytes(vector.get(position)).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class IntervalDayTimeDecoder
            implements VectorTypeDecoder
    {
        private final IntervalDayVector vector;

        public IntervalDayTimeDecoder(IntervalDayVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            Duration duration = vector.getObject(position);
            return formatMillis(duration.toNanos() / 1_000_000);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    public interface VectorTypeDecoder
            extends Closeable
    {
        Object decode(int position);
    }

    private static VectorTypeDecoder debugging(VectorTypeDecoder delegate)
    {
        if (!DEBUG) {
            return delegate;
        }
        return new VectorTypeDecoder() {
            @Override
            public Object decode(int position)
            {
                Object object = delegate.decode(position);
                System.out.println(delegate.getClass().getSimpleName() + "[" + position + "] = " + object);
                return object;
            }

            @Override
            public void close()
                    throws IOException
            {
                delegate.close();
            }
        };
    }

    private static <T extends FieldVector> T checkedCast(ValueVector vector, Class<T> clazz)
    {
        checkArgument(clazz.isInstance(vector), "Expected %s, but got %s", clazz, vector.getClass());
        return clazz.cast(vector);
    }
}
