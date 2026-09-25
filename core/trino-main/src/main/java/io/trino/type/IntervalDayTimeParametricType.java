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
package io.trino.type;

import io.trino.spi.type.IntervalField;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;

import java.util.List;

import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;

public final class IntervalDayTimeParametricType
        implements ParametricType
{
    public static final IntervalDayTimeParametricType INTERVAL_DAY_TIME_PARAMETRIC = new IntervalDayTimeParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.INTERVAL_DAY_TO_SECOND;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return INTERVAL_DAY_TIME;
        }
        if (parameters.size() == 2) {
            return createIntervalDayTimeType(field(parameters.get(0)), field(parameters.get(1)));
        }
        if (parameters.size() == 3) {
            return createIntervalDayTimeType(field(parameters.get(0)), field(parameters.get(1)), precision(parameters.get(2)));
        }
        if (parameters.size() == 4) {
            return createIntervalDayTimeType(field(parameters.get(0)), field(parameters.get(1)), precision(parameters.get(2)), precision(parameters.get(3)));
        }
        throw new IllegalArgumentException("Expected a start field, end field, leading precision, and fractional-seconds precision for a day-time interval qualifier");
    }

    private static IntervalField field(TypeParameter parameter)
    {
        if (!(parameter instanceof TypeParameter.Numeric(long code))) {
            throw new IllegalArgumentException("Interval qualifier field must be a number");
        }
        return IntervalField.fromCode((int) code);
    }

    private static int precision(TypeParameter parameter)
    {
        if (!(parameter instanceof TypeParameter.Numeric(long precision))) {
            throw new IllegalArgumentException("Interval leading precision must be a number");
        }
        return (int) precision;
    }
}
