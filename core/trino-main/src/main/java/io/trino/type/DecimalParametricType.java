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

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;

import java.util.List;

public class DecimalParametricType
        implements ParametricType
{
    public static final DecimalParametricType DECIMAL = new DecimalParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.DECIMAL;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        return switch (parameters.size()) {
            case 0 -> DecimalType.createDecimalType();
            case 1 -> DecimalType.createDecimalType(parameters.get(0).getLongLiteral().intValue());
            case 2 -> DecimalType.createDecimalType(parameters.get(0).getLongLiteral().intValue(), parameters.get(1).getLongLiteral().intValue());
            default -> throw new IllegalArgumentException("Expected 0, 1 or 2 parameters for DECIMAL type constructor.");
        };
    }
}
