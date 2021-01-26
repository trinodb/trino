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

import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.ParameterKind;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RowParametricType
        implements ParametricType
{
    public static final RowParametricType ROW = new RowParametricType();

    private RowParametricType()
    {
    }

    @Override
    public String getName()
    {
        return StandardTypes.ROW;
    }

    @Override
    public Type createType(TypeManager typeManager, List<TypeParameter> parameters)
    {
        checkArgument(!parameters.isEmpty(), "Row type must have at least one parameter");
        checkArgument(
                parameters.stream().allMatch(parameter -> parameter.getKind() == ParameterKind.NAMED_TYPE),
                "Expected only named types as a parameters, got %s",
                parameters);

        List<TypeSignatureParameter> typeSignatureParameters = parameters.stream()
                .map(TypeParameter::getNamedType)
                .map(parameter -> TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(parameter.getName(), parameter.getType().getTypeSignature())))
                .collect(toList());

        List<RowType.Field> fields = parameters.stream()
                .map(TypeParameter::getNamedType)
                .map(parameter -> new RowType.Field(parameter.getName().map(RowFieldName::getName), parameter.getType()))
                .collect(toList());

        return RowType.createWithTypeSignature(new TypeSignature(StandardTypes.ROW, typeSignatureParameters), fields);
    }
}
