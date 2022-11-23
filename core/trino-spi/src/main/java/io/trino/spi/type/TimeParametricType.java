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

import java.util.List;

public class TimeParametricType
        implements ParametricType
{
    public static final TimeParametricType TIME = new TimeParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.TIME;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return TimeType.TIME_MILLIS;
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one parameter for TIME");
        }

        TypeParameter parameter = parameters.get(0);

        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("TIME precision must be a number");
        }

        long precision = parameter.getLongLiteral();

        if (precision < 0 || precision > TimeType.MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid TIME precision " + precision);
        }

        return TimeType.createTimeType((int) precision);
    }
}
