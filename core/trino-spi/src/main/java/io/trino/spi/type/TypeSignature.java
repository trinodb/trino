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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.Immutable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static io.trino.spi.type.StandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TypeParameter.typeParameter;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toUnmodifiableList;

@Immutable
public final class TypeSignature
{
    private static final String TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone";
    private static final String TIMESTAMP_WITHOUT_TIME_ZONE = "timestamp without time zone";

    private final String base;
    private final List<TypeParameter> parameters;
    private final boolean calculated;

    private int hashCode;

    public TypeSignature(String base, TypeParameter... parameters)
    {
        this(base, asList(parameters));
    }

    public TypeSignature(String base, List<TypeParameter> parameters)
    {
        checkArgument(base != null, "base is null");
        this.base = base;
        checkArgument(!base.isEmpty(), "base is empty");
        checkArgument(validateName(base), "Bad characters in base type: %s", base);
        checkArgument(parameters != null, "parameters is null");
        this.parameters = List.copyOf(parameters);

        this.calculated = parameters.stream().anyMatch(TypeParameter::isCalculated);
    }

    public String getBase()
    {
        return base;
    }

    public List<TypeParameter> getParameters()
    {
        return parameters;
    }

    public List<TypeSignature> getTypeParametersAsTypeSignatures()
    {
        List<TypeSignature> result = new ArrayList<>();
        for (TypeParameter parameter : parameters) {
            if (parameter instanceof TypeParameter.Type(_, TypeSignature type)) {
                result.add(type);
            }
            else {
                throw new IllegalStateException(format("Expected all parameters to be TypeSignatures but [%s] was found", parameter));
            }
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
                parameters.get(0) instanceof TypeParameter.Numeric(long length) &&
                length == VarcharType.UNBOUNDED_LENGTH) {
            return base;
        }

        // TODO: this is somewhat of a hack. We need to evolve TypeSignature to be more "structural" for the special types, similar to DataType from the AST.
        //   In fact. TypeSignature should become the IR counterpart to DataType from the AST.
        if (base.equalsIgnoreCase(TIMESTAMP_WITH_TIME_ZONE)) {
            return format("timestamp(%s) with time zone", parameters.get(0));
        }

        if (base.equalsIgnoreCase(TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return format("timestamp(%s) without time zone", parameters.get(0));
        }

        if (base.equalsIgnoreCase(TIME_WITH_TIME_ZONE)) {
            return format("time(%s) with time zone", parameters.get(0));
        }

        StringBuilder typeName = new StringBuilder(base);
        typeName.append("(").append(json ? parameters.get(0).jsonValue() : parameters.get(0).toString());
        for (int i = 1; i < parameters.size(); i++) {
            typeName.append(",").append(json ? parameters.get(i).jsonValue() : parameters.get(i).toString());
        }
        typeName.append(")");
        return typeName.toString();
    }

    @FormatMethod
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
        int hash = hashCode;
        if (hash == 0) {
            hash = Objects.hash(base.toLowerCase(Locale.ENGLISH), parameters);
            if (hash == 0) {
                hash = 1;
            }
            hashCode = hash;
        }

        return hash;
    }

    // Type signature constructors for common types

    public static TypeSignature arrayType(TypeSignature elementType)
    {
        return new TypeSignature(StandardTypes.ARRAY, typeParameter(elementType));
    }

    public static TypeSignature arrayType(TypeParameter elementType)
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
                Arrays.stream(parameters)
                        .map(TypeParameter::typeParameter)
                        .collect(toUnmodifiableList()));
    }

    public static TypeSignature functionType(TypeSignature first, TypeSignature... rest)
    {
        List<TypeParameter> parameters = new ArrayList<>();
        parameters.add(typeParameter(first));

        Arrays.stream(rest)
                .map(TypeParameter::typeParameter)
                .forEach(parameters::add);

        return new TypeSignature("function", parameters);
    }

    public static TypeSignature rowType(TypeParameter... fields)
    {
        return rowType(Arrays.asList(fields));
    }

    public static TypeSignature rowType(List<TypeParameter> fields)
    {
        checkArgument(fields.stream().allMatch(parameter -> parameter instanceof TypeParameter.Type), "Parameters for ROW type must be TYPE parameters");

        return new TypeSignature(StandardTypes.ROW, fields);
    }
}
