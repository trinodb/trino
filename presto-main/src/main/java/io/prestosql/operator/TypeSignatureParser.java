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
package io.prestosql.operator;

import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.TypeSignatureParameter.typeParameter;
import static java.lang.Character.isDigit;
import static java.lang.String.format;

public class TypeSignatureParser
{
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_]([a-zA-Z0-9_:@])*");
    private static final Set<String> SIMPLE_TYPE_WITH_SPACES =
            new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        SIMPLE_TYPE_WITH_SPACES.add(StandardTypes.TIME_WITH_TIME_ZONE);
        SIMPLE_TYPE_WITH_SPACES.add(StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
        SIMPLE_TYPE_WITH_SPACES.add(StandardTypes.INTERVAL_DAY_TO_SECOND);
        SIMPLE_TYPE_WITH_SPACES.add(StandardTypes.INTERVAL_YEAR_TO_MONTH);
        SIMPLE_TYPE_WITH_SPACES.add("double precision");
    }

    private TypeSignatureParser() {}

    public static TypeSignature parseTypeSignature(String signature, Set<String> literalCalculationParameters)
    {
        if (!signature.contains("<") && !signature.contains("(")) {
            if (signature.equalsIgnoreCase(StandardTypes.VARCHAR)) {
                return VarcharType.createUnboundedVarcharType().getTypeSignature();
            }
            checkArgument(!literalCalculationParameters.contains(signature), "Bad type signature: '%s'", signature);
            return new TypeSignature(signature, new ArrayList<>());
        }
        if (signature.toLowerCase(Locale.ENGLISH).startsWith(StandardTypes.ROW + "(")) {
            return parseRowTypeSignature(signature, literalCalculationParameters);
        }

        String baseName = null;
        List<TypeSignatureParameter> parameters = new ArrayList<>();
        int parameterStart = -1;
        int bracketCount = 0;

        for (int i = 0; i < signature.length(); i++) {
            char c = signature.charAt(i);
            if (c == '(') {
                if (bracketCount == 0) {
                    verify(baseName == null, "Expected baseName to be null");
                    verify(parameterStart == -1, "Expected parameter start to be -1");
                    baseName = signature.substring(0, i);
                    checkArgument(!literalCalculationParameters.contains(baseName), "Bad type signature: '%s'", signature);
                    parameterStart = i + 1;
                }
                bracketCount++;
            }
            else if (c == ')') {
                bracketCount--;
                checkArgument(bracketCount >= 0, "Bad type signature: '%s'", signature);
                if (bracketCount == 0) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i, literalCalculationParameters));
                    parameterStart = i + 1;
                    if (i == signature.length() - 1) {
                        return new TypeSignature(baseName, parameters);
                    }
                }
            }
            else if (c == ',') {
                if (bracketCount == 1) {
                    checkArgument(parameterStart >= 0, "Bad type signature: '%s'", signature);
                    parameters.add(parseTypeSignatureParameter(signature, parameterStart, i, literalCalculationParameters));
                    parameterStart = i + 1;
                }
            }
        }

        throw new IllegalArgumentException(format("Bad type signature: '%s'", signature));
    }

    private enum RowTypeSignatureParsingState
    {
        START_OF_FIELD,
        DELIMITED_NAME,
        DELIMITED_NAME_ESCAPED,
        TYPE_OR_NAMED_TYPE,
        TYPE,
        FINISHED,
    }

    private static TypeSignature parseRowTypeSignature(String signature, Set<String> literalParameters)
    {
        checkArgument(signature.toLowerCase(Locale.ENGLISH).startsWith(StandardTypes.ROW + "("), "Not a row type signature: '%s'", signature);

        RowTypeSignatureParsingState state = RowTypeSignatureParsingState.START_OF_FIELD;
        int bracketLevel = 1;
        int tokenStart = -1;
        String delimitedColumnName = null;

        List<TypeSignatureParameter> fields = new ArrayList<>();

        for (int i = StandardTypes.ROW.length() + 1; i < signature.length(); i++) {
            char c = signature.charAt(i);
            switch (state) {
                case START_OF_FIELD:
                    if (c == '"') {
                        state = RowTypeSignatureParsingState.DELIMITED_NAME;
                        tokenStart = i;
                    }
                    else if (isValidStartOfIdentifier(c)) {
                        state = RowTypeSignatureParsingState.TYPE_OR_NAMED_TYPE;
                        tokenStart = i;
                    }
                    else {
                        checkArgument(c == ' ', "Bad type signature: '%s'", signature);
                    }
                    break;

                case DELIMITED_NAME:
                    if (c == '"') {
                        if (i + 1 < signature.length() && signature.charAt(i + 1) == '"') {
                            state = RowTypeSignatureParsingState.DELIMITED_NAME_ESCAPED;
                        }
                        else {
                            // Remove quotes around the delimited column name
                            verify(tokenStart >= 0, "Expect tokenStart to be non-negative");
                            delimitedColumnName = signature.substring(tokenStart + 1, i);
                            tokenStart = i + 1;
                            state = RowTypeSignatureParsingState.TYPE;
                        }
                    }
                    break;

                case DELIMITED_NAME_ESCAPED:
                    verify(c == '"', "Expect quote after escape");
                    state = RowTypeSignatureParsingState.DELIMITED_NAME;
                    break;

                case TYPE_OR_NAMED_TYPE:
                    if (c == '(') {
                        bracketLevel++;
                    }
                    else if (c == ')' && bracketLevel > 1) {
                        bracketLevel--;
                    }
                    else if (c == ')') {
                        verify(tokenStart >= 0, "Expect tokenStart to be non-negative");
                        fields.add(parseTypeOrNamedType(signature.substring(tokenStart, i).trim(), literalParameters));
                        tokenStart = -1;
                        state = RowTypeSignatureParsingState.FINISHED;
                    }
                    else if (c == ',' && bracketLevel == 1) {
                        verify(tokenStart >= 0, "Expect tokenStart to be non-negative");
                        fields.add(parseTypeOrNamedType(signature.substring(tokenStart, i).trim(), literalParameters));
                        tokenStart = -1;
                        state = RowTypeSignatureParsingState.START_OF_FIELD;
                    }
                    break;

                case TYPE:
                    if (c == '(') {
                        bracketLevel++;
                    }
                    else if (c == ')' && bracketLevel > 1) {
                        bracketLevel--;
                    }
                    else if (c == ')') {
                        verify(tokenStart >= 0, "Expect tokenStart to be non-negative");
                        verify(delimitedColumnName != null, "Expect delimitedColumnName to be non-null");
                        fields.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(
                                Optional.of(new RowFieldName(delimitedColumnName, true)),
                                parseTypeSignature(signature.substring(tokenStart, i).trim(), literalParameters))));
                        delimitedColumnName = null;
                        tokenStart = -1;
                        state = RowTypeSignatureParsingState.FINISHED;
                    }
                    else if (c == ',' && bracketLevel == 1) {
                        verify(tokenStart >= 0, "Expect tokenStart to be non-negative");
                        verify(delimitedColumnName != null, "Expect delimitedColumnName to be non-null");
                        fields.add(TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(
                                Optional.of(new RowFieldName(delimitedColumnName, true)),
                                parseTypeSignature(signature.substring(tokenStart, i).trim(), literalParameters))));
                        delimitedColumnName = null;
                        tokenStart = -1;
                        state = RowTypeSignatureParsingState.START_OF_FIELD;
                    }
                    break;

                case FINISHED:
                    throw new IllegalStateException(format("Bad type signature: '%s'", signature));

                default:
                    throw new AssertionError(format("Unexpected RowTypeSignatureParsingState: %s", state));
            }
        }

        checkArgument(state == RowTypeSignatureParsingState.FINISHED, "Bad type signature: '%s'", signature);
        return new TypeSignature(signature.substring(0, StandardTypes.ROW.length()), fields);
    }

    private static TypeSignatureParameter parseTypeOrNamedType(String typeOrNamedType, Set<String> literalParameters)
    {
        int split = typeOrNamedType.indexOf(' ');

        // Type without space or simple type with spaces
        if (split == -1 || SIMPLE_TYPE_WITH_SPACES.contains(typeOrNamedType)) {
            return TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.empty(), parseTypeSignature(typeOrNamedType, literalParameters)));
        }

        // Assume the first part of a structured type always has non-alphabetical character.
        // If the first part is a valid identifier, parameter is a named field.
        String firstPart = typeOrNamedType.substring(0, split);
        if (IDENTIFIER_PATTERN.matcher(firstPart).matches()) {
            return TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(
                    Optional.of(new RowFieldName(firstPart, false)),
                    parseTypeSignature(typeOrNamedType.substring(split + 1).trim(), literalParameters)));
        }

        // Structured type composed from types with spaces. i.e. array(timestamp with time zone)
        return TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.empty(), parseTypeSignature(typeOrNamedType, literalParameters)));
    }

    private static TypeSignatureParameter parseTypeSignatureParameter(
            String signature,
            int begin,
            int end,
            Set<String> literalCalculationParameters)
    {
        String parameterName = signature.substring(begin, end).trim();
        if (isDigit(signature.charAt(begin))) {
            return TypeSignatureParameter.numericParameter(Long.parseLong(parameterName));
        }
        else if (literalCalculationParameters.contains(parameterName)) {
            return TypeSignatureParameter.typeVariable(parameterName);
        }
        else {
            return typeParameter(parseTypeSignature(parameterName, literalCalculationParameters));
        }
    }

    private static boolean isValidStartOfIdentifier(char c)
    {
        return (c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                c == '_';
    }
}
