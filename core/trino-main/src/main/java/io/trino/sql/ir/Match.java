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
package io.trino.sql.ir;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.IrUtils.validateType;
import static java.util.Objects.requireNonNull;

@JsonSerialize
public record Match(Expression operand, List<MatchClause> clauses, Expression defaultValue)
        implements Expression
{
    public Match
    {
        requireNonNull(operand, "operand is null");
        requireNonNull(defaultValue, "defaultValue is null");
        clauses = ImmutableList.copyOf(clauses);
        checkArgument(!clauses.isEmpty(), "clauses is empty");

        FunctionType expectedPredicateType = new FunctionType(List.of(operand.type()), BOOLEAN);
        for (MatchClause clause : clauses) {
            // The predicate is a Lambda or a Bind around one — both report a FunctionType, and
            // capture desugaring keeps the outward shape unchanged because Bind reduces the
            // wrapped lambda's arity by exactly the number of captured values.
            checkArgument(clause.predicate().type().equals(expectedPredicateType),
                    "predicate type %s does not match expected %s",
                    clause.predicate().type(),
                    expectedPredicateType);
        }

        for (int i = 1; i < clauses.size(); i++) {
            validateType(clauses.getFirst().result().type(), clauses.get(i).result());
        }

        validateType(clauses.getFirst().result().type(), defaultValue);
    }

    @Override
    public Type type()
    {
        return clauses.getFirst().result().type();
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatch(this, context);
    }

    @Override
    public List<? extends Expression> children()
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.<Expression>builder()
                .add(operand);

        clauses.forEach(clause -> {
            builder.add(clause.predicate());
            builder.add(clause.result());
        });

        builder.add(defaultValue);
        return builder.build();
    }
}
