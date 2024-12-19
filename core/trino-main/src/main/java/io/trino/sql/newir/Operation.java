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

import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static io.trino.sql.newir.PrinterOptions.INDENT;
import static io.trino.sql.newir.Value.validateValueName;
import static java.util.stream.Collectors.joining;

/**
 * Operation is the main building block of a program.
 * Operation, as well as other code elements, does not use JSON serialization.
 * The serialized format is obtained through the print() method.
 */
public non-sealed interface Operation
        extends SourceNode
{
    record Result(String name, Type type)
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

    String name();

    Result result();

    List<Value> arguments();

    List<Region> regions();

    Map<String, Object> attributes();

    default String print(int indentLevel)
    {
        StringBuilder builder = new StringBuilder();
        String indent = INDENT.repeat(indentLevel);

        builder.append(indent)
                .append(result().name())
                .append(" = ")
                .append(name())
                .append(arguments().stream()
                        .map(Value::name)
                        .collect(joining(", ", "(", ")")))
                .append(" : ")
                .append(arguments().stream()
                        .map(Value::type)
                        .map(Type::toString)
                        .collect(joining(", ", "(", ")")))
                .append(" -> ")
                .append(result().type().toString())
                .append(regions().stream()
                        .map(region -> region.print(indentLevel + 1))
                        .collect(joining(", ", " (", ")")));

        // do not render empty attributes list
        if (!attributes().isEmpty()) {
            builder.append("\n")
                    .append(indent)
                    .append(INDENT)
                    .append(attributes().entrySet().stream()
                            .map(entry -> entry.getKey() + " = " + entry.getValue().toString())
                            .collect(joining(", ", "{", "}")));
        }

        return builder.toString();
    }

    default String prettyPrint(int indentLevel)
    {
        return print(indentLevel);
    }
}
