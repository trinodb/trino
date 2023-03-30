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

public abstract sealed class TimeWithTimeZoneType
        extends AbstractType
        implements FixedWidthType
        permits LongTimeWithTimeZoneType, ShortTimeWithTimeZoneType
{
    public static final int MAX_PRECISION = 12;
    public static final int MAX_SHORT_PRECISION = 9;

    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    private static final TimeWithTimeZoneType[] TYPES = new TimeWithTimeZoneType[MAX_PRECISION + 1];

    static {
        for (int precision = 0; precision <= MAX_PRECISION; precision++) {
            TYPES[precision] = (precision <= MAX_SHORT_PRECISION) ? new ShortTimeWithTimeZoneType(precision) : new LongTimeWithTimeZoneType(precision);
        }
    }

    public static final TimeWithTimeZoneType TIME_TZ_SECONDS = createTimeWithTimeZoneType(0);
    public static final TimeWithTimeZoneType TIME_TZ_MILLIS = createTimeWithTimeZoneType(3);
    public static final TimeWithTimeZoneType TIME_TZ_MICROS = createTimeWithTimeZoneType(6);
    public static final TimeWithTimeZoneType TIME_TZ_NANOS = createTimeWithTimeZoneType(9);
    public static final TimeWithTimeZoneType TIME_TZ_PICOS = createTimeWithTimeZoneType(12);

    /**
     * @deprecated Use {@link #createTimeWithTimeZoneType} instead.
     */
    @Deprecated
    // Use singleton for backwards compatibility with code checking `type == TIME_WITH_TIME_ZONE`
    public static final TimeWithTimeZoneType TIME_WITH_TIME_ZONE = TIME_TZ_MILLIS;

    private final int precision;

    public static TimeWithTimeZoneType createTimeWithTimeZoneType(int precision)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIME WITH TIME ZONE precision must be in range [0, %s]: %s", MAX_PRECISION, precision));
        }
        return TYPES[precision];
    }

    protected TimeWithTimeZoneType(int precision, Class<?> javaType)
    {
        super(new TypeSignature(StandardTypes.TIME_WITH_TIME_ZONE, TypeSignatureParameter.numericParameter(precision)), javaType);
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
