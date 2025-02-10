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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.TrinoException;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.Operation.Result;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.newir.FormatOptions.validateVersion;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Representation of a program in the spirit of MLIR.
 * The top-level entity is an Operation which has a Block enclosing the logic of the program.
 * The valueMap is a mapping of each Value to its source. Value names are unique.
 */
@Immutable
public final class Program
{
    private final Operation root;

    // each Operation.Result is mapped to its returning Operation
    // each Block.Parameter is mapped to its declaring Block
    private final Map<Value, SourceNode> valueMap;

    public Program(Operation root, Map<Value, SourceNode> valueMap)
    {
        this.root = requireNonNull(root, "root is null");
        this.valueMap = ImmutableMap.copyOf(requireNonNull(valueMap, "valueMap is null"));
    }

    public Operation getOperation(Result value)
    {
        SourceNode source = valueMap.get(value);
        if (source == null) {
            throw new TrinoException(IR_ERROR, format("value %s is not defined", value.name()));
        }

        if (source instanceof Operation operation) {
            if (!value.type().equals(operation.result().type())) {
                throw new TrinoException(IR_ERROR, format("value %s type mismatch. expected: %s, actual: %s", value.name(), value.type(), operation.result().type()));
            }
            return operation;
        }

        throw new TrinoException(IR_ERROR, format("value %s is not an operation result", value.name()));
    }

    public Block getBlock(Parameter value)
    {
        SourceNode source = valueMap.get(value);
        if (source == null) {
            throw new TrinoException(IR_ERROR, format("value %s is not defined", value.name()));
        }

        if (source instanceof Block block) {
            Type parameterType = block.parameters().get(block.getIndex(value)).type();
            if (!value.type().equals(parameterType)) {
                throw new TrinoException(IR_ERROR, format("value %s type mismatch. expected: %s, actual: %s", value.name(), value.type(), parameterType));
            }
            return block;
        }

        throw new TrinoException(IR_ERROR, format("value %s is not a block parameter", value.name()));
    }

    public Operation getRoot()
    {
        return root;
    }

    public Map<Value, SourceNode> getValueMap()
    {
        return valueMap;
    }

    public String print(int version, FormatOptions formatOptions)
    {
        validateVersion(version);
        return "IR version = " + version + "\n" + root.print(version, 0, formatOptions) + "\n";
    }
}
