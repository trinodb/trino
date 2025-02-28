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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.optimizations.CteReuse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.FIELD_NAME;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.assignRelationRowTypeFieldNames;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class Project
        extends Operation
{
    private static final String NAME = "project";

    private final Result result;
    private final Value input;
    private final Region assignments;
    private final Map<AttributeKey, Object> attributes;

    public Project(String resultName, Value input, Block assignments, Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(assignments, "assignments is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Project operation must be of relation type");
        }
        this.input = input;

        if (assignments.parameters().size() != 1 ||
                !trinoType(assignments.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !(trinoType(assignments.getReturnedType()) instanceof RowType || trinoType(assignments.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid assignments for Project operation");
        }
        this.assignments = singleBlockRegion(assignments);

        Type resultType;
        if (trinoType(assignments.getReturnedType()).equals(EMPTY_ROW)) {
            resultType = new MultisetType(EMPTY_ROW);
        }
        else {
            resultType = new MultisetType(assignRelationRowTypeFieldNames((RowType) trinoType(assignments.getReturnedType())));
        }
        this.result = new Result(resultName, irType(resultType));

        this.attributes = ImmutableMap.of(); // TODO
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
        return ImmutableList.of(assignments);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty project";
    }

    public Block assignments()
    {
        return assignments.getOnlyBlock();
    }

    public boolean isPruning(Map<Value, SourceNode> valueMap)
    {
        if (relationRowType(trinoType(result.type())).equals(EMPTY_ROW)) {
            // prunes all fields
            return true;
        }
        Block assignments = assignments();
        Row rowConstructor = (Row) valueMap.get(((Return) assignments.getTerminalOperation()).argument());
        for (Value rowElement : rowConstructor.arguments()) {
            SourceNode assignment = valueMap.get(rowElement);
            if (!(assignment instanceof FieldSelection fieldSelection) || !valueMap.get(fieldSelection.base()).equals(assignments)) {
                return false;
            }
        }
        return true;
    }

    // TODO this should take the Program and do scoped resolution of values within the Project's asignments
    public CteReuse.Mapping passthroughMapping()
    {
        if (relationRowType(trinoType(input.type())).equals(EMPTY_ROW) || relationRowType(trinoType(result.type())).equals(EMPTY_ROW)) {
            return new CteReuse.Mapping(ImmutableMap.of(), ImmutableMap.of()); // TODO Mapping.identity(EMPTY_ROW) or ad static constant EMPTY_MAPPING, which is identity
        }

        List<String> inputFields = ((RowType) relationRowType(trinoType(input.type()))).getFields().stream()
                .map(RowType.Field::getName)
                .map(Optional::orElseThrow)
                .collect(toImmutableList());
        List<String> outputFields = ((RowType) relationRowType(trinoType(result.type()))).getFields().stream()
                .map(RowType.Field::getName)
                .map(Optional::orElseThrow)
                .collect(toImmutableList());

        ImmutableMap.Builder<Integer, Integer> indexMapping = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> nameMapping = ImmutableMap.builder();
        List<Operation> assignmentBlockOperations = assignments().operations();
        Row rowConstructor = (Row) assignmentBlockOperations.get(assignmentBlockOperations.size() - 2);
        for (Operation assignment : assignmentBlockOperations.subList(0, assignmentBlockOperations.size() - 2)) {
            if (assignment instanceof FieldSelection fieldSelection &&
                    getOnlyElement(fieldSelection.arguments()).equals(getOnlyElement(assignments().parameters()))) {
                String inputName = FIELD_NAME.getAttribute(fieldSelection.attributes());
                int inputIndex = inputFields.indexOf(inputName);
                int outputIndex = rowConstructor.arguments().indexOf(fieldSelection.result());
                String outputName = outputFields.get(outputIndex);
                indexMapping.put(inputIndex, outputIndex);
                nameMapping.put(inputName, outputName);
            }
        }
        return new CteReuse.Mapping(indexMapping.buildOrThrow(), nameMapping.buildOrThrow());
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
        var that = (Project) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.assignments, that.assignments) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, assignments, attributes);
    }
}
