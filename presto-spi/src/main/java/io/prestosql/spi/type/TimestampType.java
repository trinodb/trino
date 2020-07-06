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
package io.prestosql.spi.type;

import io.prestosql.spi.PrestoException;

import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;

/**
 * A timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC and is to be interpreted as date-time in UTC.
 * In legacy timestamp semantics, timestamp is stored as milliseconds from 1970-01-01T00:00:00 UTC and is to be
 * interpreted in session time zone.
 */
public abstract class TimestampType
        extends AbstractType
        implements FixedWidthType
{
    public static final int MAX_PRECISION = 12;

    public static final int MAX_SHORT_PRECISION = 6;
    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    @Deprecated
    public static final TimestampType TIMESTAMP = new ShortTimestampType(DEFAULT_PRECISION);

    private final int precision;

    public static TimestampType createTimestampType(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Use singleton for backwards compatibility with code checking `type == TIMESTAMP`
            return TIMESTAMP;
        }

        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP precision must be in range [0, %s]", MAX_PRECISION));
        }

        if (precision <= MAX_SHORT_PRECISION) {
            return new ShortTimestampType(precision);
        }

        return new LongTimestampType(precision);
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
