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

import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TypeSignatureParameter.numericParameter;
import static java.lang.String.format;

public abstract sealed class DecimalType
        extends AbstractType
        implements FixedWidthType
        permits LongDecimalType, ShortDecimalType
{
    public static final int DEFAULT_SCALE = 0;
    public static final int DEFAULT_PRECISION = MAX_PRECISION;

    public static DecimalType createDecimalType(int precision, int scale)
    {
        if (precision <= 0 || precision > MAX_PRECISION) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("DECIMAL precision must be in range [1, %d]: %s", MAX_PRECISION, precision));
        }

        if (scale < 0 || scale > precision) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("DECIMAL scale must be in range [0, precision (%s)]: %s", precision, scale));
        }

        if (precision <= MAX_SHORT_PRECISION) {
            return ShortDecimalType.getInstance(precision, scale);
        }
        return new LongDecimalType(precision, scale);
    }

    public static DecimalType createDecimalType(int precision)
    {
        return createDecimalType(precision, DEFAULT_SCALE);
    }

    public static DecimalType createDecimalType()
    {
        return createDecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
    }

    private final int precision;
    private final int scale;

    DecimalType(int precision, int scale, Class<?> javaType)
    {
        super(new TypeSignature(StandardTypes.DECIMAL, buildTypeParameters(precision, scale)), javaType);
        this.precision = precision;
        this.scale = scale;
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

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public boolean isShort()
    {
        return precision <= MAX_SHORT_PRECISION;
    }

    private static List<TypeSignatureParameter> buildTypeParameters(int precision, int scale)
    {
        return List.of(numericParameter(precision), numericParameter(scale));
    }

    static void checkArgument(boolean condition, String format, Object... args)
    {
        if (!condition) {
            throw new IllegalArgumentException(format(format, args));
        }
    }
}
