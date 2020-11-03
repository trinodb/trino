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

import java.util.List;

public final class TSRangeParametricType
        implements ParametricType
{
    public static final TSRangeParametricType TSRANGE = new TSRangeParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.TSRANGE;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        if (parameters.isEmpty()) {
            return TSRangeType.createTSRangeType(TSRangeType.DEFAULT_PRECISION);
        }
        if (parameters.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one parameter for tsrange");
        }

        TypeParameter parameter = parameters.get(0);

        if (!parameter.isLongLiteral()) {
            throw new IllegalArgumentException("tsrange precision must be a number");
        }

        long precision = parameter.getLongLiteral();

        if (precision < 0 || precision > TSRangeType.MAX_PRECISION || precision % 3 != 0) {
            throw new IllegalArgumentException("Invalid tsrange precision " + precision);
        }

        return TSRangeType.createTSRangeType((int) precision);
    }
}
