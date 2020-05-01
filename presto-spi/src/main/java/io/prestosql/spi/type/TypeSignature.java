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

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.TypeSignatureParameter.typeParameter;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class TypeSignature
{
    private final String base;
    private final List<TypeSignatureParameter> parameters;
    private final boolean calculated;

    public TypeSignature(String base, TypeSignatureParameter... parameters)
    {
        this(base, asList(parameters));
    }

    public TypeSignature(String base, List<TypeSignatureParameter> parameters)
    {
        checkArgument(base != null, "base is null");
        this.base = base;
        checkArgument(!base.isEmpty(), "base is empty");
        checkArgument(validateName(base), "Bad characters in base type: %s", base);
        checkArgument(parameters != null, "parameters is null");
        this.parameters = unmodifiableList(new ArrayList<>(parameters));

        this.calculated = parameters.stream().anyMatch(TypeSignatureParameter::isCalculated);
    }

    public String getBase()
    {
        return base;
    }

    public List<TypeSignatureParameter> getParameters()
    {
        return parameters;
    }

    public List<TypeSignature> getTypeParametersAsTypeSignatures()
    {
        List<TypeSignature> result = new ArrayList<>();
        for (TypeSignatureParameter parameter : parameters) {
            if (parameter.getKind() != ParameterKind.TYPE) {
                throw new IllegalStateException(
                        format("Expected all parameters to be TypeSignatures but [%s] was found", parameter.toString()));
            }
            result.add(parameter.getTypeSignature());
        }
        return result;
    }

    public boolean isCalculated()
    {
        return calculated;
    }

    @Override
    public String toString()
    {
        return formatValue(false);
    }

    @JsonValue
    public String jsonValue()
    {
        return formatValue(true);
    }

    private String formatValue(boolean json)
    {
        if (parameters.isEmpty()) {
            return base;
        }

        if (base.equalsIgnoreCase(StandardTypes.VARCHAR) &&
                (parameters.size() == 1) &&
                parameters.get(0).isLongLiteral() &&
                parameters.get(0).getLongLiteral() == VarcharType.UNBOUNDED_LENGTH) {
            return base;
        }

        StringBuilder typeName = new StringBuilder(base);
        typeName.append("(").append(json ? parameters.get(0).jsonValue() : parameters.get(0).toString());
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(json ? parameters.get(i).jsonValue() : parameters.get(i).toString());
        }
        typeName.append(")");
        return typeName.toString();
    }

    private static void checkArgument(boolean argument, String format, Object... args)
    {
        if (!argument) {
            throw new IllegalArgumentException(format(format, args));
        }
    }

    private static boolean validateName(String name)
    {
        return name.chars().noneMatch(c -> c == '<' || c == '>' || c == ',');
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TypeSignature other = (TypeSignature) o;

        return Objects.equals(this.base.toLowerCase(Locale.ENGLISH), other.base.toLowerCase(Locale.ENGLISH)) &&
                Objects.equals(this.parameters, other.parameters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base.toLowerCase(Locale.ENGLISH), parameters);
    }

    // Type signature constructors for common types

    public static TypeSignature arrayType(TypeSignature elementType)
    {
        return new TypeSignature(StandardTypes.ARRAY, typeParameter(elementType));
    }

    public static TypeSignature arrayType(TypeSignatureParameter elementType)
    {
        return new TypeSignature(StandardTypes.ARRAY, elementType);
    }

    public static TypeSignature mapType(TypeSignature keyType, TypeSignature valueType)
    {
        return new TypeSignature(StandardTypes.MAP, typeParameter(keyType), typeParameter(valueType));
    }

    public static TypeSignature parametricType(String name, TypeSignature... parameters)
    {
        return new TypeSignature(
                name,
                Arrays.asList(parameters).stream()
                        .map(TypeSignatureParameter::typeParameter)
                        .collect(Collectors.toList()));
    }

    public static TypeSignature functionType(TypeSignature first, TypeSignature... rest)
    {
        List<TypeSignatureParameter> parameters = new ArrayList<>();
        parameters.add(typeParameter(first));

        Arrays.asList(rest).stream()
                .map(TypeSignatureParameter::typeParameter)
                .forEach(parameters::add);

        return new TypeSignature("function", parameters);
    }

    public static TypeSignature rowType(TypeSignatureParameter... fields)
    {
        return rowType(Arrays.asList(fields));
    }

    public static TypeSignature rowType(List<TypeSignatureParameter> fields)
    {
        checkArgument(fields.stream().allMatch(parameter -> parameter.getKind() == ParameterKind.NAMED_TYPE), "Parameters for ROW type must be NAMED_TYPE parameters");

        return new TypeSignature(StandardTypes.ROW, fields);
    }
}
