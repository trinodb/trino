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
package io.trino.sql.gen;

import io.trino.metadata.Metadata;
import io.trino.sql.ir.Array;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Let;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;

import java.util.List;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

final class ConditionalPredication
{
    private ConditionalPredication() {}

    /**
     * A conditional (e.g. AND/OR) can be compiled to branchless, predicated form only when every operand is
     * safe to evaluate unconditionally: the feature is enabled and each term is deterministic and provably
     * infallible. Evaluating a term that short-circuiting would have skipped must not change the result, so
     * it must neither throw nor have an observable side effect on a row it would not otherwise reach.
     */
    static boolean canPredicate(BytecodeGeneratorContext generator, List<Expression> terms)
    {
        if (!generator.isConditionalPredicationEnabled()) {
            return false;
        }
        Metadata metadata = generator.getMetadata();
        return terms.stream().allMatch(term -> isDeterministic(term) && neverFails(term, metadata));
    }

    /**
     * Codegen-local infallibility check: returns true only when {@code expression} provably cannot throw.
     * Mirrors {@link io.trino.sql.ir.IrExpressions#mayFail(io.trino.sql.PlannerContext, Expression)} with one deliberate
     * exception: a {@link Lambda} is treated as fallible because its body is evaluated when the lambda is
     * invoked, which this static check cannot reason about. The switch is exhaustive over the sealed
     * {@link Expression} type, so a new node forces a conscious decision here rather than silently
     * defaulting.
     */
    private static boolean neverFails(Expression expression, Metadata metadata)
    {
        return switch (expression) {
            case Constant _, Reference _, FieldReference _, Bind _ -> true;
            // a lambda body is evaluated on invocation, which this check cannot see, so treat it as fallible
            case Lambda _ -> false;
            case Array e -> e.elements().stream().allMatch(element -> neverFails(element, metadata));
            case Call e -> e.function().neverFails() && e.arguments().stream().allMatch(argument -> neverFails(argument, metadata));
            case Case e -> e.whenClauses().stream().allMatch(clause -> neverFails(clause.getOperand(), metadata) && neverFails(clause.getResult(), metadata)) && neverFails(e.defaultValue(), metadata);
            case Cast e -> metadata.getCoercion(e.expression().type(), e.type()).neverFails() && neverFails(e.expression(), metadata);
            case Coalesce e -> e.operands().stream().allMatch(operand -> neverFails(operand, metadata));
            case In e -> neverFails(e.value(), metadata) && e.valueList().stream().allMatch(value -> neverFails(value, metadata));
            case IsNull e -> neverFails(e.value(), metadata);
            case Let e -> neverFails(e.value(), metadata) && neverFails(e.body(), metadata);
            case Logical e -> e.terms().stream().allMatch(term -> neverFails(term, metadata));
            case Match e -> neverFails(e.operand(), metadata) && e.clauses().stream().allMatch(clause -> neverFails(clause.lambda().body(), metadata) && neverFails(clause.result(), metadata)) && neverFails(e.defaultValue(), metadata);
            case Row e -> e.items().stream().allMatch(item -> neverFails(item, metadata));
        };
    }
}
