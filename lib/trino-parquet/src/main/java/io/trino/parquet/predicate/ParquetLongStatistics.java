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
package io.trino.parquet.predicate;

import io.trino.spi.type.DecimalType;
import org.apache.parquet.io.api.Binary;

import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;

public class ParquetLongStatistics
        implements ParquetRangeStatistics<Long>
{
    public static ParquetLongStatistics fromBinary(DecimalType type, Binary minimum, Binary maximum)
    {
        if (!type.isShort()) {
            throw new IllegalArgumentException("Cannot convert long DecimalType to ParquetLongStatistics");
        }
        return new ParquetLongStatistics(getShortDecimalValue(minimum.getBytes()), getShortDecimalValue(maximum.getBytes()));
    }

    public static ParquetLongStatistics fromNumber(Number minimum, Number maximum)
    {
        if (minimum instanceof Double || minimum instanceof Float) {
            throw unsupportedType(minimum);
        }
        if (maximum instanceof Double || maximum instanceof Float) {
            throw unsupportedType(maximum);
        }
        return new ParquetLongStatistics(minimum.longValue(), maximum.longValue());
    }

    private static IllegalArgumentException unsupportedType(Number number)
    {
        return new IllegalArgumentException("Disallowed inexact conversion to ParquetLongStatistics from " + number.getClass().getName());
    }

    private final Long minimum;
    private final Long maximum;

    private ParquetLongStatistics(Long minimum, Long maximum)
    {
        this.minimum = minimum;
        this.maximum = maximum;
    }

    @Override
    public Long getMin()
    {
        return minimum;
    }

    @Override
    public Long getMax()
    {
        return maximum;
    }
}
