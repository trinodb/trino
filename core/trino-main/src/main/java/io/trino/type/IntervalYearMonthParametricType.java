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

import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.IntervalYearMonthType.createIntervalYearMonthType;

public final class IntervalYearMonthParametricType
        implements ParametricType
{
    public static final IntervalYearMonthParametricType INTERVAL_YEAR_MONTH_PARAMETRIC = new IntervalYearMonthParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.INTERVAL_YEAR_TO_MONTH;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return INTERVAL_YEAR_MONTH;
        }
        if (parameters.size() == 2) {
            return createIntervalYearMonthType(field(parameters.get(0)), field(parameters.get(1)));
        }
        if (parameters.size() == 3) {
            return createIntervalYearMonthType(field(parameters.get(0)), field(parameters.get(1)), precision(parameters.get(2)));
        }
        throw new IllegalArgumentException("Expected a start field, end field, and optional leading precision for a year-month interval qualifier");
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
