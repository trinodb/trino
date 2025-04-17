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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.server.protocol.spooling.SpooledBlock.SPOOLING_METADATA_SYMBOL;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.ir.IrDialect.terminalOperation;
import static io.trino.sql.dialect.trino.Attributes.OUTPUT_NAMES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Output
        extends TrinoOperation
{
    private static final String NAME = "output";

    private final Result result;
    private final Value input;
    private final Region fieldSelector;
    private final Map<AttributeKey, Object> attributes;

    public Output(String resultName, Value input, Block fieldSelector, List<String> outputNames)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(fieldSelector, "fieldSelector is null");
        requireNonNull(outputNames, "outputNames is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Output operation must be of relation type");
        }

        this.result = new Result(resultName, irType(BOOLEAN)); // returning boolean to avoid introducing type void

        this.input = input;

        if (fieldSelector.parameters().size() != 1 ||
                !trinoType(fieldSelector.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                trinoType(fieldSelector.parameters().getFirst().type()).equals(EMPTY_ROW) ||
                !(trinoType(fieldSelector.getReturnedType()) instanceof RowType rowType)) {
            throw new TrinoException(IR_ERROR, "invalid field selection for Output operation");
        }

        // the Output operation selects fields from the input through the fieldSelector block,
        // and optionally adds a spooling metadata symbol indicated by an additional name in the outputNames list
        int selectedFieldsSize = rowType.getTypeParameters().size();
        if (selectedFieldsSize == outputNames.size() - 1) {
            if (!outputNames.getLast().equals(SPOOLING_METADATA_SYMBOL.name())) {
                throw new TrinoException(IR_ERROR, "invalid spooling metadata field for Output operation");
            }
        }
        else if (selectedFieldsSize != outputNames.size()) {
            throw new TrinoException(IR_ERROR, "invalid field selection for Output operation");
        }

        this.fieldSelector = singleBlockRegion(fieldSelector);

        ImmutableMap.Builder<AttributeKey, Object> attributesBuilder = ImmutableMap.builder();
        OUTPUT_NAMES.putAttribute(attributesBuilder, outputNames);
        terminalOperation(attributesBuilder);

        this.attributes = attributesBuilder.buildOrThrow();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of(fieldSelector);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty output";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Output(
                result.name(),
                newArgument,
                fieldSelector.getOnlyBlock(),
                OUTPUT_NAMES.getAttribute(attributes));
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (Output) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.fieldSelector, that.fieldSelector) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, fieldSelector, attributes);
    }
}
