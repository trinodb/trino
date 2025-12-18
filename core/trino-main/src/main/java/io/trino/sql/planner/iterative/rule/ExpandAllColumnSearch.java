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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.type.LikePatternType;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.type.LikeFunctions.LIKE_FUNCTION_NAME;
import static io.trino.type.LikeFunctions.LIKE_PATTERN_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

/**
 * Expands allcolumnsearch() function calls in WHERE clauses into explicit column searches.
 * <p>
 * Transforms:
 * <pre>
 *     SELECT * FROM table WHERE allcolumnsearch('term')
 * </pre>
 * Into:
 * <pre>
 *     SELECT * FROM table WHERE col1 LIKE '%term%' OR col2 LIKE '%term%' OR ...
 * </pre>
 * <p>
 * Performs case-sensitive substring matching using LIKE predicates.
 * Generates simple LIKE predicates without lower() for maximum database pushdown.
 * <p>
 * Only searches VARCHAR/CHAR columns and excludes hidden columns.
 */
public class ExpandAllColumnSearch
{
    private final PlannerContext plannerContext;

    public ExpandAllColumnSearch(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(new ExpandAllColumnSearchRule(plannerContext));
    }

    private static class ExpandAllColumnSearchRule
            implements Rule<FilterNode>
    {
        private static final Pattern<FilterNode> PATTERN = filter();

        private final PlannerContext plannerContext;

        public ExpandAllColumnSearchRule(PlannerContext plannerContext)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            Session session = context.getSession();
            Metadata metadata = plannerContext.getMetadata();
            PlanNode source = filterNode.getSource();

            // Check if the predicate contains allcolumnsearch - if not, skip this rule
            if (!containsAllColumnSearch(filterNode.getPredicate())) {
                return Result.empty();
            }

            // Rewrite the predicate expression
            Expression rewrittenPredicate = new AllColumnSearchRewriter(
                    session,
                    source,
                    metadata,
                    plannerContext.getFunctionManager(),
                    context
            ).process(filterNode.getPredicate());

            // If nothing changed, return empty result
            if (rewrittenPredicate.equals(filterNode.getPredicate())) {
                return Result.empty();
            }

            return Result.ofPlanNode(new FilterNode(
                    filterNode.getId(),
                    filterNode.getSource(),
                    rewrittenPredicate));
        }

        private static boolean containsAllColumnSearch(Expression expression)
        {
            if (expression instanceof Call call) {
                if (call.function().name().equals(builtinFunctionName("allcolumnsearch"))) {
                    return true;
                }
            }
            // Recursively check children
            for (Expression child : expression.children()) {
                if (containsAllColumnSearch(child)) {
                    return true;
                }
            }
            return false;
        }

        private static class AllColumnSearchRewriter
                extends IrVisitor<Expression, Void>
        {
            private final Session session;
            private final List<Symbol> availableSymbols;
            private final Metadata metadata;
            private final Rule.Context context;

            public AllColumnSearchRewriter(Session session, io.trino.sql.planner.plan.PlanNode sourceNode, Metadata metadata, io.trino.metadata.FunctionManager functionManager, Rule.Context context)
            {
                this.session = requireNonNull(session, "session is null");
                this.availableSymbols = sourceNode.getOutputSymbols();
                this.metadata = requireNonNull(metadata, "metadata is null");
                this.context = requireNonNull(context, "context is null");
            }

            @Override
            protected Expression visitCall(Call node, Void context)
            {
                // Check if this is an allcolumnsearch() call
                if (node.function().name().equals(builtinFunctionName("allcolumnsearch"))) {
                    return expandAllColumnSearch(node);
                }

                // Recursively process children
                List<Expression> rewrittenChildren = node.arguments().stream()
                        .map(child -> process(child, context))
                        .collect(toImmutableList());

                // If any children changed, reconstruct the expression
                if (!node.arguments().equals(rewrittenChildren)) {
                    return new Call(node.function(), rewrittenChildren);
                }

                return node;
            }

            @Override
            protected Expression visitLogical(Logical node, Void context)
            {
                List<Expression> rewrittenTerms = node.terms().stream()
                        .map(term -> process(term, context))
                        .collect(toImmutableList());

                if (!node.terms().equals(rewrittenTerms)) {
                    return new Logical(node.operator(), rewrittenTerms);
                }

                return node;
            }

            @Override
            protected Expression visitCast(Cast node, Void context)
            {
                Expression rewrittenExpression = process(node.expression(), context);
                if (!node.expression().equals(rewrittenExpression)) {
                    return new Cast(rewrittenExpression, node.type());
                }
                return node;
            }

            @Override
            protected Expression visitExpression(Expression node, Void context)
            {
                // For other expression types, don't recurse (we only expect allcolumnsearch at top level or in logical/cast)
                return node;
            }

            private Expression expandAllColumnSearch(Call allColumnSearchCall)
            {
                // Validate the call has exactly one argument
                if (allColumnSearchCall.arguments().size() != 1) {
                    throw new TrinoException(
                            StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                            "allcolumnsearch() requires exactly one argument");
                }

                // Get the search term - must be a constant (possibly wrapped in a Cast)
                Expression searchTermExpr = allColumnSearchCall.arguments().get(0);

                // Unwrap Cast if present
                if (searchTermExpr instanceof Cast cast) {
                    searchTermExpr = cast.expression();
                }

                if (!(searchTermExpr instanceof Constant searchTermConstant)) {
                    throw new TrinoException(
                            StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                            "allcolumnsearch() requires a constant VARCHAR search term");
                }

                // Extract the search term value
                if (searchTermConstant.value() == null) {
                    throw new TrinoException(
                            StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                            "allcolumnsearch() search term cannot be NULL");
                }

                Slice searchTerm = (Slice) searchTermConstant.value();
                String searchString = searchTerm.toStringUtf8();

                // Validate search term is not empty
                if (searchString.isEmpty()) {
                    throw new TrinoException(
                            StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                            "allcolumnsearch() search term cannot be empty");
                }

                // Find all searchable symbols (VARCHAR/CHAR types)
                List<Symbol> searchableSymbols = availableSymbols.stream()
                        .filter(symbol -> isStringType(symbol.type()))
                        .collect(toImmutableList());

                // If no searchable columns, return false
                if (searchableSymbols.isEmpty()) {
                    return new Constant(BOOLEAN, false);
                }

                // Create search pattern with wildcards (preserving case for case-sensitive search)
                Constant searchPattern = new Constant(VARCHAR, utf8Slice("%" + searchString + "%"));

                // Resolve the like pattern function once (same for all columns)
                ResolvedFunction likePatternFunction = metadata.resolveBuiltinFunction(LIKE_PATTERN_FUNCTION_NAME, fromTypes(VARCHAR));

                // Build OR expression for all searchable columns
                ImmutableList.Builder<Expression> columnPredicates = ImmutableList.builderWithExpectedSize(searchableSymbols.size());

                for (Symbol symbol : searchableSymbols) {
                    // Build: col LIKE '%term%' (case-sensitive)
                    // This simple form enables best database pushdown:
                    // - No function calls on columns
                    // - Direct column reference allows index usage where supported
                    // - Direct LIKE without lower() for better performance
                    Reference columnRef = new Reference(symbol.type(), symbol.name());

                    // Create a fresh likePatternCall for each column
                    Call likePatternCall = new Call(likePatternFunction, ImmutableList.of(searchPattern));

                    // Resolve the $like function which takes (varchar/char, LikePattern) -> boolean
                    ResolvedFunction likeFunction = metadata.resolveBuiltinFunction(LIKE_FUNCTION_NAME, fromTypes(symbol.type(), LikePatternType.LIKE_PATTERN));
                    Expression likePredicate = new Call(likeFunction, ImmutableList.of(columnRef, likePatternCall));

                    columnPredicates.add(likePredicate);
                }

                // Combine all predicates with OR
                return buildOrExpression(columnPredicates.build());
            }

            private Expression buildOrExpression(List<Expression> predicates)
            {
                if (predicates.isEmpty()) {
                    return new Constant(BOOLEAN, false);
                }
                if (predicates.size() == 1) {
                    return predicates.get(0);
                }

                // Build a chain of OR expressions
                Expression result = predicates.get(0);
                for (int i = 1; i < predicates.size(); i++) {
                    result = Logical.or(result, predicates.get(i));
                }
                return result;
            }

            private boolean isStringType(Type type)
            {
                return type instanceof VarcharType || type instanceof CharType;
            }
        }
    }
}
