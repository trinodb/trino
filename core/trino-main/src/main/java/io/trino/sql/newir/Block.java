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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.TERMINAL;
import static io.trino.sql.newir.FormatOptions.INDENT;
import static io.trino.sql.newir.FormatOptions.isValidPrefixedIdentifier;
import static io.trino.sql.newir.Value.validateValueName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * A Block is a unit of code consisting of a list of Operations.
 * A Block belongs to a Region, and together with other Blocks within a Region it forms a Control Flow Graph.
 * Note that for now, we only support single-block Regions, so no control flow is involved.
 * Blocks define Parameters being typed values.
 * Blocks must end with a terminal operation such as Return.
 * Blocks have an optional name (label). They should be accessed by their position in the list within a Region.
 */
public record Block(Optional<String> name, List<Parameter> parameters, List<Operation> operations)
        implements SourceNode
{
    public record Parameter(String name, Type type)
            implements Value
    {
        public Parameter
        {
            validateValueName(name);
        }

        @Override
        public Block source(Program program)
        {
            return program.getBlock(this);
        }
    }

    public Block(Optional<String> name, List<Parameter> parameters, List<Operation> operations)
    {
        this.name = requireNonNull(name, "name is null");
        validateBlockName(name);
        this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        this.operations = ImmutableList.copyOf(requireNonNull(operations, "operations is null"));
        if (operations.isEmpty()) {
            throw new TrinoException(IR_ERROR, "invalid block: empty operations list");
        }
        Object terminalAttribute = operations.getLast().attributes().get(new AttributeKey(IR, TERMINAL));
        if (terminalAttribute == null) {
            throw new TrinoException(IR_ERROR, "invalid block: last operation not terminal");
        }
        if (!terminalAttribute.equals(true)) {
            throw new TrinoException(IR_ERROR, "invalid block: last operation terminal attribute value should be true. actual: " + terminalAttribute);
        }
    }

    private static void validateBlockName(Optional<String> optionalName)
    {
        optionalName.ifPresent(name -> {
            if (!name.startsWith("^") || !isValidPrefixedIdentifier(name.substring(1))) {
                throw new TrinoException(IR_ERROR, format("invalid block name: \"%s\"", name));
            }
        });
    }

    public String print(int version, int indentLevel, FormatOptions formatOptions)
    {
        StringBuilder builder = new StringBuilder();
        String indent = INDENT.repeat(indentLevel);

        builder.append(indent)
                .append(name().orElse(""));

        if (!parameters().isEmpty()) {
            builder.append(parameters().stream()
                    .map(parameter -> parameter.name() + " : " + formatOptions.formatType(version, parameter.type()))
                    .collect(joining(", ", " (", ")")));
        }

        builder.append(operations().stream()
                .map(operation -> operation.print(version, indentLevel + 1, formatOptions))
                .collect(joining("\n", "\n", "")));

        return builder.toString();
    }

    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return print(1, indentLevel, formatOptions);
    }

    public int getIndex(Parameter parameter)
    {
        int index = parameters.indexOf(parameter);
        if (index < 0) {
            throw new TrinoException(IR_ERROR, parameter.name() + "is not a parameter of given block");
        }
        return index;
    }

    public Operation getTerminalOperation()
    {
        return operations.getLast();
    }

    public Type getReturnedType()
    {
        return getTerminalOperation().result().type();
    }

    public static class Builder
    {
        private final Optional<String> name;
        private final List<Parameter> parameters;
        private final ImmutableList.Builder<Operation> operations = ImmutableList.builder();
        private Optional<Operation> recentOperation = Optional.empty();

        public Builder(Optional<String> name, List<Parameter> parameters)
        {
            this.name = name;
            this.parameters = parameters;
        }

        public Builder addOperation(Operation operation)
        {
            operations.add(operation);
            recentOperation = Optional.of(operation);
            return this;
        }

        // access to the recently added operation allows the caller to append a return operation or a navigating operation (in the future)
        public Operation recentOperation()
        {
            return recentOperation.orElseThrow(() -> new TrinoException(IR_ERROR, "no operations added yet"));
        }

        public Block build()
        {
            return new Block(name, parameters, operations.build());
        }
    }
}
