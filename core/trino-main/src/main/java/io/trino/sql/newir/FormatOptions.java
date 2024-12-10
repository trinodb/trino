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
package io.trino.sql.newir;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static java.util.Objects.requireNonNull;

public class FormatOptions
{
    public static final String INDENT = "    ";
    private static final Pattern IDENTIFIER = Pattern.compile("([a-z]|[A-Z]|_)([a-z]|[A-Z]|[0-9]|_)*");
    private static final Pattern PREFIXED_IDENTIFIER = Pattern.compile("([a-z]|[A-Z]|[0-9]|_)+");

    private final DialectRegistry dialectRegistry;

    @Inject
    public FormatOptions(DialectRegistry dialectRegistry)
    {
        this.dialectRegistry = requireNonNull(dialectRegistry, "dialectRegistry is null");
    }

    public static void validateVersion(int version)
    {
        if (version != 1) {
            throw new TrinoException(IR_ERROR, "invalid format version: " + version);
        }
    }

    public static boolean isValidIdentifier(String identifier)
    {
        return IDENTIFIER.matcher(identifier).matches();
    }

    public static boolean isValidPrefixedIdentifier(String identifier)
    {
        return PREFIXED_IDENTIFIER.matcher(identifier).matches();
    }

    public static String formatName(int version, Operation operation)
    {
        if (version == 1 && operation.dialect().equals(TRINO)) {
            return operation.name();
        }
        return operation.dialect() + "." + operation.name();
    }

    public String formatAttribute(int version, AttributeKey key, Object attribute)
    {
        String dialectPrefix;
        if (version == 1 && key.dialect().equals(TRINO)) {
            dialectPrefix = "";
        }
        else {
            dialectPrefix = key.dialect() + ".";
        }

        return dialectPrefix + key.name() + " = " + quote(dialectRegistry.dialect(key.dialect()).formatAttribute(key.name(), attribute));
    }

    public Object parseAttribute(int version, Optional<String> dialect, String name, String attribute)
    {
        String dialectName;
        if (version == 1) {
            dialectName = dialect.orElse(TRINO);
        }
        else {
            dialectName = dialect.orElseThrow(() -> new TrinoException(IR_ERROR, "missing dialect name for an attribute in IR version " + version));
        }

        return dialectRegistry.dialect(dialectName).parseAttribute(name, unquote(attribute));
    }

    public String formatType(int version, Type type)
    {
        String dialectPrefix;
        if (version == 1 && type.dialect().equals(TRINO)) {
            dialectPrefix = "";
        }
        else {
            dialectPrefix = type.dialect() + ".";
        }

        return dialectPrefix + quote(dialectRegistry.dialect(type.dialect()).formatType(type));
    }

    public Type parseType(int version, Optional<String> dialect, String type)
    {
        String dialectName;
        if (version == 1) {
            dialectName = dialect.orElse(TRINO);
        }
        else {
            dialectName = dialect.orElseThrow(() -> new TrinoException(IR_ERROR, "missing dialect name for a type in IR version " + version));
        }

        return dialectRegistry.dialect(dialectName).parseType(unquote(type));
    }

    private static String quote(String string)
    {
        return "\"" + string.replace("\"", "\"\"") + "\"";
    }

    private static String unquote(String string)
    {
        return string.substring(1, string.length() - 1)
                .replace("\"\"", "\"");
    }
}
