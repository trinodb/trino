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

public abstract class TimestampWithTimeZoneType
        extends AbstractType
        implements FixedWidthType
{
    public static final int MAX_PRECISION = 12;

    public static final int MAX_SHORT_PRECISION = 3;
    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    @Deprecated
    public static final TimestampWithTimeZoneType TIMESTAMP_WITH_TIME_ZONE = new ShortTimestampWithTimeZoneType(DEFAULT_PRECISION);

    private final int precision;

    public static TimestampWithTimeZoneType createTimestampWithTimeZoneType(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Use singleton for backwards compatibility with code checking `type == TIMESTAMP_WITH_TIME_ZONE`
            return TIMESTAMP_WITH_TIME_ZONE;
        }

        if (precision <= MAX_SHORT_PRECISION) {
            return new ShortTimestampWithTimeZoneType(precision);
        }

        if (precision <= MAX_PRECISION) {
            return new LongTimestampWithTimeZoneType(precision);
        }

        throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIMESTAMP WITH TIME ZONE precision must be in range [0, %s]", MAX_PRECISION));
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
