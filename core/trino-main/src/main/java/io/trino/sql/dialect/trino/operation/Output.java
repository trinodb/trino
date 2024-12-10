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
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.Attributes.OutputNames;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.VoidType.VOID;
import static io.trino.sql.dialect.trino.Attributes.OUTPUT_NAMES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Output
        implements Operation
{
    private static final String NAME = "output";

    private final Result result;
    private final Value input;
    private final Region fieldSelector;
    private final List<String> outputNames;

    public Output(String resultName, Value input, Block fieldSelector, List<String> outputNames)
    {
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(fieldSelector, "fieldSelector is null");
        requireNonNull(outputNames, "outputNames is null");

        if (!IS_RELATION.test(input.type())) {
            throw new TrinoException(IR_ERROR, "input to the Output operation must be of relation type");
        }

        this.result = new Result(resultName, VOID);

        this.input = input;

        if (fieldSelector.parameters().size() != 1 ||
                !fieldSelector.parameters().getFirst().type().equals(relationRowType(input.type())) ||
                fieldSelector.parameters().getFirst().type().equals(EMPTY_ROW) ||
                !(fieldSelector.getReturnedType() instanceof RowType) ||
                ((RowType) fieldSelector.getReturnedType()).getTypeParameters().size() != outputNames.size()) {
            throw new TrinoException(IR_ERROR, "invalid field selection for Output operation");
        }

        this.fieldSelector = singleBlockRegion(fieldSelector);

        this.outputNames = outputNames;
    }

    @Override
    public String name()
    {
        return NAME;
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
    public Map<String, Object> attributes()
    {
        return OUTPUT_NAMES.asMap(new OutputNames(outputNames));
    }

    @Override
    public String prettyPrint(int indentLevel)
    {
        return "pretty output";
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {return true;}
        if (obj == null || obj.getClass() != this.getClass()) {return false;}
        var that = (Output) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.fieldSelector, that.fieldSelector) &&
                Objects.equals(this.outputNames, that.outputNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, fieldSelector, outputNames);
    }
}
