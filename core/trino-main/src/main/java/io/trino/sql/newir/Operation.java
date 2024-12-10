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

import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.newir.Dialect.validateDialectName;
import static io.trino.sql.newir.FormatOptions.INDENT;
import static io.trino.sql.newir.FormatOptions.formatName;
import static io.trino.sql.newir.FormatOptions.isValidIdentifier;
import static io.trino.sql.newir.Value.validateValueName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Operation is the main building block of a program.
 * Operation, as well as other code elements, does not use JSON serialization.
 * The serialized format is obtained through the print() method.
 */
public abstract non-sealed class Operation
        implements SourceNode
{
    private final String dialect;
    private final String name;

    public Operation(String dialect, String name)
    {
        requireNonNull(dialect, "dialect is null");
        validateDialectName(dialect);
        this.dialect = dialect;

        requireNonNull(name, "name is null");
        if (!isValidIdentifier(name)) {
            throw new TrinoException(IR_ERROR, format("invalid operation name: \"%s\"", name));
        }
        this.name = name;
    }

    public record Result(String name, Type type)
            implements Value
    {
        public Result
        {
            validateValueName(name);
        }

        @Override
        public Operation source(Program program)
        {
            return program.getOperation(this);
        }
    }

    public record AttributeKey(String dialect, String name)
    {
        public AttributeKey
        {
            validateDialectName(dialect);
            if (!isValidIdentifier(name)) {
                throw new TrinoException(IR_ERROR, format("invalid attribute name: \"%s\"", name));
            }
        }
    }

    public final String dialect()
    {
        return dialect;
    }

    public final String name()
    {
        return name;
    }

    public abstract Result result();

    public abstract List<Value> arguments();

    public abstract List<Region> regions();

    public abstract Map<AttributeKey, Object> attributes();

    public final String print(int version, int indentLevel, FormatOptions formatOptions)
    {
        StringBuilder builder = new StringBuilder();
        String indent = INDENT.repeat(indentLevel);

        builder.append(indent)
                .append(result().name())
                .append(" = ")
                .append(formatName(version, this))
                .append(arguments().stream()
                        .map(Value::name)
                        .collect(joining(", ", "(", ")")))
                .append(" : ")
                .append(arguments().stream()
                        .map(Value::type)
                        .map(type -> formatOptions.formatType(version, type))
                        .collect(joining(", ", "(", ")")))
                .append(" -> ")
                .append(formatOptions.formatType(version, result().type()))
                .append(regions().stream()
                        .map(region -> region.print(version, indentLevel + 1, formatOptions))
                        .collect(joining(", ", " (", ")")));

        // do not render empty attributes list
        if (!attributes().isEmpty()) {
            builder.append("\n")
                    .append(indent)
                    .append(INDENT)
                    .append(attributes().entrySet().stream()
                            .map(entry -> formatOptions.formatAttribute(version, entry.getKey(), entry.getValue()))
                            .collect(joining(", ", "{", "}")));
        }

        return builder.toString();
    }

    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return print(1, indentLevel, formatOptions);
    }
}
