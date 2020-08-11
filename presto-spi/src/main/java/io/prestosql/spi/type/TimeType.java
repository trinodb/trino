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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;

import static io.prestosql.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static java.lang.String.format;

/**
 * A time is stored as picoseconds from midnight.
 */
public final class TimeType
        extends AbstractLongType
{
    public static final int MAX_PRECISION = 12;
    public static final int DEFAULT_PRECISION = 3; // TODO: should be 6 per SQL spec

    @Deprecated
    public static final TimeType TIME = new TimeType(DEFAULT_PRECISION);

    private final int precision;

    private TimeType(int precision)
    {
        super(new TypeSignature(StandardTypes.TIME, TypeSignatureParameter.numericParameter(precision)));
        this.precision = precision;
    }

    public static TimeType createTimeType(int precision)
    {
        if (precision == DEFAULT_PRECISION) {
            // Use singleton for backwards compatibility with code checking `type == TIME`
            return TIME;
        }

        if (precision < 0 || precision > MAX_PRECISION) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, format("TIME precision must be in range [0, %s]", MAX_PRECISION));
        }

        return new TimeType(precision);
    }

    public int getPrecision()
    {
        return precision;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return SqlTime.newInstance(precision, block.getLong(position, 0));
    }
}
