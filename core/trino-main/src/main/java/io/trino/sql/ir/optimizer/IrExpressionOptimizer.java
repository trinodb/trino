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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.PlannerContext;
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
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.rule.DistributeComparisonOverCase;
import io.trino.sql.ir.optimizer.rule.DistributeComparisonOverMatch;
import io.trino.sql.ir.optimizer.rule.EvaluateArray;
import io.trino.sql.ir.optimizer.rule.EvaluateBind;
import io.trino.sql.ir.optimizer.rule.EvaluateCall;
import io.trino.sql.ir.optimizer.rule.EvaluateCallWithNullInput;
import io.trino.sql.ir.optimizer.rule.EvaluateCase;
import io.trino.sql.ir.optimizer.rule.EvaluateCast;
import io.trino.sql.ir.optimizer.rule.EvaluateComparison;
import io.trino.sql.ir.optimizer.rule.EvaluateFieldReference;
import io.trino.sql.ir.optimizer.rule.EvaluateIn;
import io.trino.sql.ir.optimizer.rule.EvaluateIsNull;
import io.trino.sql.ir.optimizer.rule.EvaluateLogical;
import io.trino.sql.ir.optimizer.rule.EvaluateMatch;
import io.trino.sql.ir.optimizer.rule.EvaluateReference;
import io.trino.sql.ir.optimizer.rule.EvaluateRow;
import io.trino.sql.ir.optimizer.rule.FlattenCoalesce;
import io.trino.sql.ir.optimizer.rule.FlattenLogical;
import io.trino.sql.ir.optimizer.rule.InlineTrivialLet;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantCaseClauses;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantCoalesceArguments;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantInItems;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantLogicalTerms;
import io.trino.sql.ir.optimizer.rule.RemoveRedundantMatchClauses;
import io.trino.sql.ir.optimizer.rule.SimplifyCharLength;
import io.trino.sql.ir.optimizer.rule.SimplifyComplementaryLogicalTerms;
import io.trino.sql.ir.optimizer.rule.SimplifyContinuousInValues;
import io.trino.sql.ir.optimizer.rule.SimplifyRedundantCase;
import io.trino.sql.ir.optimizer.rule.SimplifyRedundantCast;
import io.trino.sql.ir.optimizer.rule.SimplifyStackedArithmeticNegation;
import io.trino.sql.ir.optimizer.rule.SimplifyStackedNot;
import io.trino.sql.ir.optimizer.rule.SpecializeCastWithJsonParse;
import io.trino.sql.ir.optimizer.rule.SpecializeTransformWithJsonParse;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class IrExpressionOptimizer
{
    private final List<IrOptimizerRule> rules;

    private IrExpressionOptimizer(List<IrOptimizerRule> rules)
    {
        this.rules = rules;
    }

    /**
     * Get a full expression optimizer. Performs partial evaluation and other semantic rewrites
     */
    public static IrExpressionOptimizer newOptimizer(PlannerContext context)
    {
        return new IrExpressionOptimizer(ImmutableList.of(
                new EvaluateReference(),
                new EvaluateArray(),
                new EvaluateRow(),
                new EvaluateBind(),
                new EvaluateFieldReference(),
                new InlineTrivialLet(),
                new SimplifyComplementaryLogicalTerms(context),
                new EvaluateIsNull(context),
                new EvaluateComparison(context),
                new EvaluateCast(context),
                new EvaluateMatch(context),
                new EvaluateCase(),
                new EvaluateCall(context),
                new EvaluateIn(context),
                new EvaluateCallWithNullInput(),
                new RemoveRedundantMatchClauses(context),
                new RemoveRedundantCaseClauses(),
                new RemoveRedundantInItems(context),
                new SimplifyContinuousInValues(context),
                new SimplifyRedundantCast(),
                new SimplifyCharLength(context),
                new SimplifyStackedNot(),
                new SimplifyStackedArithmeticNegation(),
                new FlattenCoalesce(),
                new RemoveRedundantCoalesceArguments(context),
                new EvaluateLogical(),
                new FlattenLogical(),
                new RemoveRedundantLogicalTerms(),
                new DistributeComparisonOverMatch(context),
                new DistributeComparisonOverCase(context),
                new SimplifyRedundantCase(context),
                new SpecializeCastWithJsonParse(context),
                new SpecializeTransformWithJsonParse(context)));
    }

    /**
     * Get an optimizer that performs partial evaluation only: it reduces an expression from known
     * constant inputs (literal operands or {@link #process} bindings). It excludes the identity- and
     * type-driven rewrites of {@link #newOptimizer}, whose result is independent of the bindings.
     */
    public static IrExpressionOptimizer newPartialEvaluator(PlannerContext context)
    {
        return new IrExpressionOptimizer(ImmutableList.of(
                new EvaluateReference(),
                new EvaluateArray(),
                new EvaluateRow(),
                new EvaluateBind(),
                new EvaluateFieldReference(),
                new InlineTrivialLet(),
                new EvaluateIsNull(context),
                new EvaluateComparison(context),
                new EvaluateCast(context),
                new EvaluateMatch(context),
                new EvaluateCase(),
                new EvaluateCall(context),
                new EvaluateIn(context),
                new EvaluateLogical()));
    }

    public Optional<Expression> process(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        boolean changed = false;

        boolean progress = true;
        while (progress) {
            progress = false;
            Optional<Expression> optimized = processChildren(expression, session, symbolAllocator, bindings);
            if (optimized.isPresent()) {
                expression = optimized.get();
                changed = true;
                progress = true;
            }

            optimized = applyRules(expression, session, symbolAllocator, bindings);
            if (optimized.isPresent()) {
                expression = optimized.get();
                changed = true;
                progress = true;
            }
        }

        return changed ? Optional.of(expression) : Optional.empty();
    }

    private Optional<Expression> processChildren(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        return switch (expression) {
            case Reference _, Constant _ -> Optional.empty();
            case Cast cast -> process(cast.expression(), session, symbolAllocator, bindings).map(value -> new Cast(value, cast.type()));
            case IsNull isNull -> process(isNull.value(), session, symbolAllocator, bindings).map(value -> new IsNull(value));
            case Logical logical -> process(logical.terms(), session, symbolAllocator, bindings).map(arguments -> new Logical(logical.operator(), arguments));
            case Call call -> process(call.arguments(), session, symbolAllocator, bindings).map(arguments -> new Call(call.function(), arguments));
            case Array array -> process(array.elements(), session, symbolAllocator, bindings).map(elements -> new Array(array.elementType(), elements));
            case Row row -> process(row.items(), session, symbolAllocator, bindings).map(fields -> new Row(fields, row.type()));
            case Coalesce coalesce -> process(coalesce.operands(), session, symbolAllocator, bindings).map(operands -> new Coalesce(operands));
            case FieldReference reference -> process(reference.base(), session, symbolAllocator, bindings).map(base -> new FieldReference(base, reference.field()));
            case In in -> {
                Optional<Expression> value = process(in.value(), session, symbolAllocator, bindings);
                Optional<List<Expression>> list = process(in.valueList(), session, symbolAllocator, bindings);
                yield value.isPresent() || list.isPresent() ?
                        Optional.of(new In(value.orElse(in.value()), list.orElse(in.valueList()))) :
                        Optional.empty();
            }
            case Lambda lambda -> process(lambda.body(), session, symbolAllocator, bindings).map(body -> new Lambda(lambda.arguments(), body));
            case Bind bind -> {
                Optional<List<Expression>> values = process(bind.values(), session, symbolAllocator, bindings);
                Optional<Expression> lambda = process(bind.function(), session, symbolAllocator, bindings);
                yield values.isPresent() || lambda.isPresent() ?
                        Optional.of(new Bind(values.orElse(bind.values()), (Lambda) lambda.orElse(bind.function()))) :
                        Optional.empty();
            }
            case Let let -> {
                Optional<Expression> value = process(let.value(), session, symbolAllocator, bindings);
                Optional<Expression> body = process(let.body(), session, symbolAllocator, bindings);
                yield value.isPresent() || body.isPresent() ?
                        Optional.of(new Let(let.name(), value.orElse(let.value()), body.orElse(let.body()))) :
                        Optional.empty();
            }
            case Match e -> {
                Optional<Expression> operand = process(e.operand(), session, symbolAllocator, bindings);
                Optional<Expression> defaultValue = process(e.defaultValue(), session, symbolAllocator, bindings);
                Optional<List<MatchClause>> clauses = processMatchClauses(e.clauses(), session, symbolAllocator, bindings);
                yield operand.isPresent() || clauses.isPresent() || defaultValue.isPresent() ?
                        Optional.of(new Match(
                                operand.orElse(e.operand()),
                                clauses.orElse(e.clauses()),
                                defaultValue.orElse(e.defaultValue()))) :
                        Optional.empty();
            }
            case Case e -> {
                Optional<Expression> defaultValue = process(e.defaultValue(), session, symbolAllocator, bindings);
                Optional<List<WhenClause>> clauses = processClauses(e.whenClauses(), session, symbolAllocator, bindings);
                yield clauses.isPresent() || defaultValue.isPresent() ?
                        Optional.of(new Case(
                                clauses.orElse(e.whenClauses()),
                                defaultValue.orElse(e.defaultValue()))) :
                        Optional.empty();
            }
        };
    }

    /**
     * @return Optional.empty() if none of the clauses changed
     */
    private Optional<List<WhenClause>> processClauses(List<WhenClause> clauses, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        boolean changed = false;
        ImmutableList.Builder<WhenClause> optimized = ImmutableList.builder();
        for (WhenClause clause : clauses) {
            Optional<Expression> operand = process(clause.getOperand(), session, symbolAllocator, bindings);
            Optional<Expression> result = process(clause.getResult(), session, symbolAllocator, bindings);
            if (operand.isPresent() || result.isPresent()) {
                optimized.add(new WhenClause(operand.orElse(clause.getOperand()), result.orElse(clause.getResult())));
            }
            else {
                optimized.add(clause);
            }
            changed = changed || operand.isPresent() || result.isPresent();
        }

        return changed ? Optional.of(optimized.build()) : Optional.empty();
    }

    /// @return Optional.empty() if none of the clauses changed
    private Optional<List<MatchClause>> processMatchClauses(List<MatchClause> clauses, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        boolean changed = false;
        ImmutableList.Builder<MatchClause> optimized = ImmutableList.builder();
        for (MatchClause clause : clauses) {
            Optional<Expression> predicate = process(clause.predicate(), session, symbolAllocator, bindings);
            Optional<Expression> result = process(clause.result(), session, symbolAllocator, bindings);
            if (predicate.isPresent() || result.isPresent()) {
                optimized.add(new MatchClause(predicate.orElse(clause.predicate()), result.orElse(clause.result())));
            }
            else {
                optimized.add(clause);
            }
            changed = changed || predicate.isPresent() || result.isPresent();
        }

        return changed ? Optional.of(optimized.build()) : Optional.empty();
    }

    /**
     * @return Optional.empty() if none of the expressions changed
     */
    private Optional<List<Expression>> process(List<Expression> expressions, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        boolean changed = false;
        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (Expression expression : expressions) {
            Optional<Expression> optimized = process(expression, session, symbolAllocator, bindings);
            changed = changed || optimized.isPresent();
            result.add(optimized.orElse(expression));
        }

        return changed ? Optional.of(result.build()) : Optional.empty();
    }

    private Optional<Expression> applyRules(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        boolean changed = false;
        for (IrOptimizerRule rule : rules) {
            Optional<Expression> optimized = rule.apply(expression, session, symbolAllocator, bindings);
            if (optimized.isPresent()) {
                checkState(
                        expression.type().equals(optimized.get().type()),
                        "Rule %s changed expression type from %s to %s",
                        rule.getClass().getSimpleName(),
                        expression.type(),
                        optimized.get().type());
                expression = optimized.get();
                changed = true;
            }
        }

        return changed ? Optional.of(expression) : Optional.empty();
    }
}
