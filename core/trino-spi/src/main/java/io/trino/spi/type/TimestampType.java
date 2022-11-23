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
 * A timestamp is to be interpreted as local date time without regards to any time zone.
 *
 * @see ShortTimestampType
 * @see LongTimestampType
 */
public abstract class TimestampType
        extends AbstractType
        implements FixedWidthType
{
    public static final int MAX_PRECISION = 12;

    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    private static final TimestampType[] TYPES = new TimestampType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TYPES[precision] = (precision <= MAX_SHORT_PRECISION) ? new ShortTimestampType(precision) : new LongTimestampType(precision);
        }
    }

    public static final TimestampType TIMESTAMP_SECONDS = createTimestampType(0);
    public static final TimestampType TIMESTAMP_MILLIS = createTimestampType(3);
    public static final TimestampType TIMESTAMP_MICROS = createTimestampType(6);
    public static final TimestampType TIMESTAMP_NANOS = createTimestampType(9);
    public static final TimestampType TIMESTAMP_PICOS = createTimestampType(12);

    private final int precision;

    public static TimestampType createTimestampType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    TimestampType(int precision, Class<?> javaType)
    {
        super(new TypeSignature(StandardTypes.TIMESTAMP, TypeSignatureParameter.numericParameter(precision)), javaType);
        this.precision = precision;
    }

    public int getPrecision()
    {
        return precision;
    }

    public final boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }
}
