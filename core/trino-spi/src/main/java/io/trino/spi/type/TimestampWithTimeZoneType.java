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
package io.trino.spi.type;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;

/**
 * @see ShortTimestampWithTimeZoneType
 * @see LongTimestampWithTimeZoneType
 */
public abstract sealed class TimestampWithTimeZoneType
        extends AbstractType
        implements FixedWidthType
        permits LongTimestampWithTimeZoneType, ShortTimestampWithTimeZoneType
{
    public static final int MAX_PRECISION = 12;

    public static final int MAX_SHORT_PRECISION = 3;
    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    private static final TimestampWithTimeZoneType[] TYPES = new TimestampWithTimeZoneType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TYPES[precision] = (precision <= MAX_SHORT_PRECISION) ? new ShortTimestampWithTimeZoneType(precision) : new LongTimestampWithTimeZoneType(precision);
        }
    }

    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_SECONDS = createTimestampWithTimeZoneType(0);
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_MILLIS = createTimestampWithTimeZoneType(3);
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_MICROS = createTimestampWithTimeZoneType(6);
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_NANOS = createTimestampWithTimeZoneType(9);
    public static final TimestampWithTimeZoneType TIMESTAMP_TZ_PICOS = createTimestampWithTimeZoneType(12);

    private final int precision;

    public static TimestampWithTimeZoneType createTimestampWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP WITH TIME ZONE precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    TimestampWithTimeZoneType(int precision, Class<?> javaType)
    {
        super(new TypeSignature(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TypeSignatureParameter.numericParameter(precision)), javaType);

        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(format("Precision must be in the range [0, %s]", MAX_PRECISION));
        }

        this.precision = precision;
    }

    public final int getPrecision()
    {
        return precision;
    }

    public final boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    @Override
    public final boolean isComparable()
    {
        return true;
    }

    @Override
    public final boolean isOrderable()
    {
        return true;
    }
}
