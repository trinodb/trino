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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Attributes.JoinType;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.Attributes.JOIN_TYPE;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.assignRelationRowTypeFieldNames;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.util.Objects.requireNonNull;

public final class CorrelatedJoin
        extends Operation
{
    private static final String NAME = "correlated_join";

    private final Result result;
    private final Value input;
    // correlation as field selector. later we should model correlation through the uses graph?
    private final Region correlation;
    private final Region subquery;
    private final Region filter;
    private final Map<AttributeKey, Object> attributes;
    // TODO the PlanNode has origin subquery for debug. skipping it for now

    public CorrelatedJoin(
            String resultName,
            Value input,
            Block correlation,
            Block subquery,
            Block filter,
            JoinType joinType,
            Map<AttributeKey, Object> sourceAttributes,
            Map<AttributeKey, Object> subqueryAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(correlation, "correlation is null");
        requireNonNull(subquery, "subquery is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(joinType, "joinType is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(subqueryAttributes, "subqueryAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type())) || !IS_RELATION.test(trinoType(subquery.getReturnedType()))) {
            throw new TrinoException(IR_ERROR, "input and subquery of CorrelatedJoin must be of relation type");
        }

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(relationRowType(trinoType(input.type())).getTypeParameters())
                .addAll(relationRowType(trinoType(subquery.getReturnedType())).getTypeParameters())
                .build();

        if (outputTypes.isEmpty()) {
            this.result = new Result(resultName, irType(new MultisetType(EMPTY_ROW)));
        }
        else {
            this.result = new Result(resultName, irType(new MultisetType(assignRelationRowTypeFieldNames(RowType.anonymous(outputTypes)))));
        }

        this.input = input;

        if (correlation.parameters().size() != 1 ||
                !trinoType(correlation.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !(trinoType(correlation.getReturnedType()) instanceof RowType || trinoType(correlation.getReturnedType()).equals(EMPTY_ROW))) {
            throw new TrinoException(IR_ERROR, "invalid correlation for CorrelatedJoin operation");
        }
        this.correlation = singleBlockRegion(correlation);

        if (subquery.parameters().size() != 1 ||
                !trinoType(subquery.parameters().getFirst().type()).equals(relationRowType(trinoType(input.type()))) ||
                !IS_RELATION.test(trinoType(subquery.getReturnedType()))) {
            throw new TrinoException(IR_ERROR, "invalid subquery for CorrelatedJoin operation");
        }
        this.subquery = singleBlockRegion(subquery);

        if (filter.parameters().size() != 2 ||
                !trinoType(filter.parameters().get(0).type()).equals(relationRowType(trinoType(input.type()))) ||
                !trinoType(filter.parameters().get(1).type()).equals(relationRowType(trinoType(subquery.getReturnedType()))) ||
                !trinoType(filter.getReturnedType()).equals(BOOLEAN)) {
            throw new TrinoException(IR_ERROR, "invalid filter for CorrelatedJoin operation");
        }
        this.filter = singleBlockRegion(filter);

        // TODO also derive attributes from source and subquery attributes
        this.attributes = JOIN_TYPE.asMap(joinType);
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
        return ImmutableList.of(correlation, subquery, filter);
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "pretty correlated join";
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
        var that = (CorrelatedJoin) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.input, that.input) &&
                Objects.equals(this.correlation, that.correlation) &&
                Objects.equals(this.subquery, that.subquery) &&
                Objects.equals(this.filter, that.filter) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, input, correlation, subquery, filter, attributes);
    }
}
