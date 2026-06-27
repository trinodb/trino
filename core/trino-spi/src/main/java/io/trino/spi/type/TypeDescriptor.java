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

import static io.trino.spi.type.TypeParameter.typeParameter;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toUnmodifiableList;

@Immutable
public final class TypeDescriptor
{
    private final String base;
    private final List<TypeParameter> parameters;
    private final boolean calculated;

    private int hashCode;

    public TypeDescriptor(String base, TypeParameter... parameters)
    {
        this(base, asList(parameters));
    }

    public TypeDescriptor(String base, List<TypeParameter> parameters)
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

    /// The internal-form [TypeId] for this type — the IR identity used at runtime (function ids,
    /// intra-cluster serialization). Durable surfaces (view columns, function signatures) instead persist
    /// the SQL spelling from [TypeSyntax] so they need no migration.
    public TypeId toTypeId()
    {
        return TypeId.of(jsonValue());
    }

    // The internal representation of a type: the standardized `base(arg, …)` form. Used for type
    // identity, function ids, serialization, and debugging — never shown to users. The user-visible SQL
    // spelling is produced separately by TypeSyntax (see Type#getTypeId and getDisplayName).
    private String formatValue(boolean json)
    {
        if (parameters.isEmpty()) {
            return base;
        }
        return base + parameters.stream()
                .map(parameter -> json ? parameter.jsonValue() : parameter.toString())
                .collect(joining(",", "(", ")"));
    }

    /// Parses the internal `base(arg, …)` IR produced by {@link #toString()}.
    public static TypeDescriptor fromString(String value)
    {
        int open = value.indexOf('(');
        if (open < 0) {
            return new TypeDescriptor(value);
        }
        checkArgument(value.charAt(value.length() - 1) == ')', "Malformed type descriptor: %s", value);
        String base = value.substring(0, open);
        String inner = value.substring(open + 1, value.length() - 1);
        List<TypeParameter> parameters = new ArrayList<>();
        for (String token : splitTopLevel(inner)) {
            parameters.add(parseParameter(token.strip()));
        }
        return new TypeDescriptor(base, parameters);
    }

    private static TypeParameter parseParameter(String token)
    {
        checkArgument(!token.isEmpty(), "Malformed type descriptor: empty parameter");
        if (isNumericLiteral(token)) {
            return TypeParameter.numericParameter(Long.parseLong(token));
        }
        if (token.startsWith("\"")) {
            int closing = closingQuote(token);
            String name = token.substring(1, closing).replace("\"\"", "\"");
            return TypeParameter.namedField(name, fromString(token.substring(closing + 1).strip()));
        }
        return TypeParameter.anonymousField(fromString(token));
    }

    private static boolean isNumericLiteral(String token)
    {
        int start = token.startsWith("-") ? 1 : 0;
        if (start == token.length()) {
            return false;
        }
        for (int i = start; i < token.length(); i++) {
            if (!Character.isDigit(token.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static int closingQuote(String token)
    {
        for (int i = 1; i < token.length(); i++) {
            if (token.charAt(i) == '"') {
                if (i + 1 < token.length() && token.charAt(i + 1) == '"') {
                    i++;
                    continue;
                }
                return i;
            }
        }
        throw new IllegalArgumentException("Unterminated quoted name in type: " + token);
    }

    private static List<String> splitTopLevel(String inner)
    {
        List<String> tokens = new ArrayList<>();
        int depth = 0;
        boolean inQuote = false;
        int start = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '"') {
                if (inQuote && i + 1 < inner.length() && inner.charAt(i + 1) == '"') {
                    i++;
                    continue;
                }
                inQuote = !inQuote;
            }
            else if (!inQuote && c == '(') {
                depth++;
            }
            else if (!inQuote && c == ')') {
                depth--;
            }
            else if (!inQuote && c == ',' && depth == 0) {
                tokens.add(inner.substring(start, i));
                start = i + 1;
            }
        }
        tokens.add(inner.substring(start));
        checkArgument(depth == 0 && !inQuote, "Malformed type descriptor: %s", inner);
        return tokens;
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

        TypeDescriptor other = (TypeDescriptor) o;

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

    // Type descriptor constructors for common types

    public static TypeDescriptor arrayType(TypeDescriptor elementType)
    {
        return new TypeDescriptor(StandardTypes.ARRAY, typeParameter(elementType));
    }

    public static TypeDescriptor arrayType(TypeParameter elementType)
    {
        return new TypeDescriptor(StandardTypes.ARRAY, elementType);
    }

    public static TypeDescriptor mapType(TypeDescriptor keyType, TypeDescriptor valueType)
    {
        return new TypeDescriptor(StandardTypes.MAP, typeParameter(keyType), typeParameter(valueType));
    }

    public static TypeDescriptor type(String name, TypeDescriptor... parameters)
    {
        return new TypeDescriptor(
                name,
                Arrays.stream(parameters)
                        .map(TypeParameter::typeParameter)
                        .collect(toUnmodifiableList()));
    }

    public static TypeDescriptor functionType(TypeDescriptor first, TypeDescriptor... rest)
    {
        List<TypeParameter> parameters = new ArrayList<>();
        parameters.add(typeParameter(first));

        Arrays.stream(rest)
                .map(TypeParameter::typeParameter)
                .forEach(parameters::add);

        return new TypeDescriptor("function", parameters);
    }

    public static TypeDescriptor rowType(List<TypeParameter> fields)
    {
        checkArgument(fields.stream().allMatch(parameter -> parameter instanceof TypeParameter.Type), "Parameters for ROW type must be TYPE parameters");

        return new TypeDescriptor(StandardTypes.ROW, fields);
    }
}
