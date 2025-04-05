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
import io.trino.sql.dialect.trino.Attributes.LogicalOperator;
import io.trino.sql.newir.FormatOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.dialect.trino.Attributes.LOGICAL_OPERATOR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static java.util.Objects.requireNonNull;

public final class Logical
        extends TrinoOperation
{
    private static final String NAME = "logical";

    private final Result result;
    private final List<Value> terms;
    private final Map<AttributeKey, Object> attributes;

    public Logical(String resultName, List<Value> terms, LogicalOperator logicalOperator, List<Map<AttributeKey, Object>> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(terms, "terms is null");
        requireNonNull(logicalOperator, "logicalOperator is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, irType(BOOLEAN));

        if (terms.size() < 2) {
            throw new TrinoException(IR_ERROR, "logical operation must have at least 2 terms. actual: " + terms.size());
        }
        terms.stream()
                .forEach(term -> {
                    if (!trinoType(term.type()).equals(BOOLEAN)) {
                        throw new TrinoException(IR_ERROR, "all terms of a logical operation must be of boolean type. found: " + trinoType(term.type()).getDisplayName());
                    }
                });
        this.terms = ImmutableList.copyOf(terms);

        // TODO also derive attributes from source attributes
        this.attributes = LOGICAL_OPERATOR.asMap(logicalOperator);
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return terms;
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, FormatOptions formatOptions)
    {
        return "logical :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newTerms = new ArrayList<>(terms);
        newTerms.set(index, newArgument);
        return new Logical(
                result.name(),
                newTerms,
                LOGICAL_OPERATOR.getAttribute(attributes),
                ImmutableList.of());
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
        var that = (Logical) obj;
        return Objects.equals(this.result, that.result) &&
                Objects.equals(this.terms, that.terms) &&
                Objects.equals(this.attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(result, terms, attributes);
    }
}
