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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.Rule.Context;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.TopNNode;
import io.prestosql.sql.planner.plan.TopNRowNumberNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.WindowNode;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.SymbolsExtractor.extractAll;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.unnest;
import static java.util.Objects.requireNonNull;

/**
 * Push down dereferences as follows:
 * <p>
 * Extract dereferences from PlanNode which has expressions
 * and push them down to a new ProjectNode right below the PlanNode.
 * After this step, All dereferences will be in ProjectNode.
 * <p>
 * Pushdown dereferences in ProjectNode down through other types of PlanNode,
 * e.g, Filter, Join etc.
 */
public class PushDownDereferences
{
    private final TypeAnalyzer typeAnalyzer;

    public PushDownDereferences(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ExtractFromFilter(typeAnalyzer),
                new ExtractFromJoin(typeAnalyzer),
                new PushDownDereferenceThrough<>(AssignUniqueId.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(WindowNode.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(TopNNode.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(RowNumberNode.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(TopNRowNumberNode.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(SortNode.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(FilterNode.class, typeAnalyzer),
                new PushDownDereferenceThrough<>(LimitNode.class, typeAnalyzer),
                new PushDownDereferenceThroughProject(typeAnalyzer),
                new PushDownDereferenceThroughUnnest(typeAnalyzer),
                new PushDownDereferenceThroughSemiJoin(typeAnalyzer),
                new PushDownDereferenceThroughJoin(typeAnalyzer));
    }

    /**
     * Extract dereferences and push them down to new ProjectNode below
     * Transforms:
     * <pre>
     *  TargetNode(expression(a.x))
     *  </pre>
     * to:
     * <pre>
     *   ProjectNode(original symbols)
     *    TargetNode(expression(symbol))
     *      Project(symbol := a.x)
     * </pre>
     */
    static class ExtractFromFilter
            implements Rule<FilterNode>
    {
        private final TypeAnalyzer typeAnalyzer;

        ExtractFromFilter(TypeAnalyzer typeAnalyzer)
        {
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode node, Captures captures, Context context)
        {
            BiMap<DereferenceExpression, Symbol> expressions =
                    HashBiMap.create(getDereferenceSymbolMap(ExpressionExtractor.extractExpressionsNonRecursive(node), context, typeAnalyzer));

            if (expressions.isEmpty()) {
                return Result.empty();
            }

            PlanNode source = node.getSource();
            Assignments assignments = Assignments.builder().putIdentities(source.getOutputSymbols()).putAll(expressions.inverse()).build();
            ProjectNode projectNode = new ProjectNode(context.getIdAllocator().getNextId(), source, assignments);

            FilterNode filterNode = new FilterNode(
                    context.getIdAllocator().getNextId(),
                    projectNode,
                    ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressions), node.getPredicate()));

            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), filterNode, Assignments.builder().putIdentities(node.getOutputSymbols()).build()));
        }
    }

    static class ExtractFromJoin
            implements Rule<JoinNode>
    {
        private final TypeAnalyzer typeAnalyzer;

        ExtractFromJoin(TypeAnalyzer typeAnalyzer)
        {
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            BiMap<DereferenceExpression, Symbol> expressions =
                    HashBiMap.create(getDereferenceSymbolMap(ExpressionExtractor.extractExpressionsNonRecursive(joinNode), context, typeAnalyzer));

            if (expressions.isEmpty()) {
                return Result.empty();
            }
            Assignments.Builder leftSideDereferences = Assignments.builder();
            Assignments.Builder rightSideDereferences = Assignments.builder();

            for (Map.Entry<Symbol, DereferenceExpression> entry : expressions.inverse().entrySet()) {
                Symbol baseSymbol = getBase(entry.getValue());
                if (joinNode.getLeft().getOutputSymbols().contains(baseSymbol)) {
                    leftSideDereferences.put(entry.getKey(), entry.getValue());
                }
                else {
                    rightSideDereferences.put(entry.getKey(), entry.getValue());
                }
            }
            PlanNode leftNode = createProjectBelow(joinNode.getLeft(), leftSideDereferences.build(), context.getIdAllocator());
            PlanNode rightNode = createProjectBelow(joinNode.getRight(), rightSideDereferences.build(), context.getIdAllocator());

            PlanNode newJoinNode = new JoinNode(
                    context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    joinNode.getCriteria(),
                    joinNode.getOutputSymbols(),
                    joinNode.getFilter().map(expression -> ExpressionTreeRewriter.rewriteWith(new PushDownDereferences.DereferenceReplacer(expressions), expression)),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType(),
                    joinNode.isSpillable(),
                    joinNode.getDynamicFilters());

            return Result.ofPlanNode(newJoinNode);
        }
    }

    /**
     * Transforms:
     * <pre>
     *  Project(a_x := a.x)
     *    TargetNode(a)
     *  </pre>
     * to:
     * <pre>
     *  Project(a_x := symbol)
     *    TargetNode(symbol)
     *      Project(symbol := a.x)
     * </pre>
     */
    static class PushDownDereferenceThrough<N extends PlanNode>
            implements Rule<ProjectNode>
    {
        private final Capture<N> targetCapture = newCapture();
        private final Pattern<N> targetPattern;

        private final TypeAnalyzer typeAnalyzer;

        PushDownDereferenceThrough(Class<N> aClass, TypeAnalyzer typeAnalyzer)
        {
            targetPattern = Pattern.typeOf(requireNonNull(aClass, "aClass is null"));
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(targetPattern.capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            N child = captures.get(targetCapture);
            Map<DereferenceExpression, Symbol> pushdownDereferences = getPushdownDereferences(context, node, captures.get(targetCapture), typeAnalyzer);

            if (pushdownDereferences.isEmpty()) {
                return Result.empty();
            }

            PlanNode source = getOnlyElement(child.getSources());

            ProjectNode projectNode = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    source,
                    Assignments.builder().putIdentities(source.getOutputSymbols()).putAll(HashBiMap.create(pushdownDereferences).inverse()).build());

            PlanNode newChildNode = child.replaceChildren(ImmutableList.of(projectNode));
            Assignments assignments = node.getAssignments().rewrite(new DereferenceReplacer(pushdownDereferences));
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newChildNode, assignments));
        }
    }

    /**
     * Transforms:
     * <pre>
     *  Project(a_x := a.msg.x)
     *    Join(a_y = b_y) => [a]
     *      Project(a_y := a.msg.y)
     *          Source(a)
     *      Project(b_y := b.msg.y)
     *          Source(b)
     *  </pre>
     * to:
     * <pre>
     *  Project(a_x := symbol)
     *    Join(a_y = b_y) => [symbol]
     *      Project(symbol := a.msg.x, a_y := a.msg.y)
     *        Source(a)
     *      Project(b_y := b.msg.y)
     *        Source(b)
     * </pre>
     */
    static class PushDownDereferenceThroughJoin
            implements Rule<ProjectNode>
    {
        private final Capture<JoinNode> targetCapture = newCapture();
        private final TypeAnalyzer typeAnalyzer;

        PushDownDereferenceThroughJoin(TypeAnalyzer typeAnalyzer)
        {
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(join().capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            JoinNode joinNode = captures.get(targetCapture);
            Map<DereferenceExpression, Symbol> pushdownDereferences = getPushdownDereferences(context, node, captures.get(targetCapture), typeAnalyzer);

            if (pushdownDereferences.isEmpty()) {
                return Result.empty();
            }

            Assignments.Builder leftSideDereferences = Assignments.builder();
            Assignments.Builder rightSideDereferences = Assignments.builder();

            for (Map.Entry<Symbol, DereferenceExpression> entry : HashBiMap.create(pushdownDereferences).inverse().entrySet()) {
                Symbol baseSymbol = getBase(entry.getValue());
                if (joinNode.getLeft().getOutputSymbols().contains(baseSymbol)) {
                    leftSideDereferences.put(entry.getKey(), entry.getValue());
                }
                else {
                    rightSideDereferences.put(entry.getKey(), entry.getValue());
                }
            }
            PlanNode leftNode = createProjectBelow(joinNode.getLeft(), leftSideDereferences.build(), context.getIdAllocator());
            PlanNode rightNode = createProjectBelow(joinNode.getRight(), rightSideDereferences.build(), context.getIdAllocator());

            JoinNode newJoinNode = new JoinNode(context.getIdAllocator().getNextId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    joinNode.getCriteria(),
                    ImmutableList.<Symbol>builder().addAll(leftNode.getOutputSymbols()).addAll(rightNode.getOutputSymbols()).build(),
                    joinNode.getFilter(),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType(),
                    joinNode.isSpillable(),
                    joinNode.getDynamicFilters());

            Assignments assignments = node.getAssignments().rewrite(new DereferenceReplacer(pushdownDereferences));
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newJoinNode, assignments));
        }
    }

    static class PushDownDereferenceThroughSemiJoin
            implements Rule<ProjectNode>
    {
        private final Capture<SemiJoinNode> targetCapture = newCapture();
        private final TypeAnalyzer typeAnalyzer;

        PushDownDereferenceThroughSemiJoin(TypeAnalyzer typeAnalyzer)
        {
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(semiJoin().capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            SemiJoinNode semiJoinNode = captures.get(targetCapture);
            Map<DereferenceExpression, Symbol> pushdownDereferences = getPushdownDereferences(context, node, captures.get(targetCapture), typeAnalyzer);

            if (pushdownDereferences.isEmpty()) {
                return Result.empty();
            }

            Assignments.Builder filteringSourceDereferences = Assignments.builder();
            Assignments.Builder sourceDereferences = Assignments.builder();

            for (Map.Entry<Symbol, DereferenceExpression> entry : HashBiMap.create(pushdownDereferences).inverse().entrySet()) {
                Symbol baseSymbol = getBase(entry.getValue());
                if (semiJoinNode.getFilteringSource().getOutputSymbols().contains(baseSymbol)) {
                    filteringSourceDereferences.put(entry.getKey(), entry.getValue());
                }
                else {
                    sourceDereferences.put(entry.getKey(), entry.getValue());
                }
            }
            PlanNode filteringSource = createProjectBelow(semiJoinNode.getFilteringSource(), filteringSourceDereferences.build(), context.getIdAllocator());
            PlanNode source = createProjectBelow(semiJoinNode.getSource(), sourceDereferences.build(), context.getIdAllocator());

            PlanNode newSemiJoin = semiJoinNode.replaceChildren(ImmutableList.of(source, filteringSource));

            Assignments assignments = node.getAssignments().rewrite(new DereferenceReplacer(pushdownDereferences));
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newSemiJoin, assignments));
        }
    }

    static class PushDownDereferenceThroughProject
            implements Rule<ProjectNode>
    {
        private final Capture<ProjectNode> targetCapture = newCapture();

        PushDownDereferenceThroughProject(TypeAnalyzer typeAnalyzer)
        {
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        private final TypeAnalyzer typeAnalyzer;

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(project().capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            ProjectNode child = captures.get(targetCapture);
            Map<DereferenceExpression, Symbol> pushdownDereferences = getPushdownDereferences(context, node, captures.get(targetCapture), typeAnalyzer);

            if (pushdownDereferences.isEmpty()) {
                return Result.empty();
            }

            ProjectNode newChild = new ProjectNode(context.getIdAllocator().getNextId(),
                    child.getSource(),
                    Assignments.builder().putAll(child.getAssignments()).putAll(HashBiMap.create(pushdownDereferences).inverse()).build());

            Assignments assignments = node.getAssignments().rewrite(new DereferenceReplacer(pushdownDereferences));
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newChild, assignments));
        }
    }

    static class PushDownDereferenceThroughUnnest
            implements Rule<ProjectNode>
    {
        private final Capture<UnnestNode> targetCapture = newCapture();

        private final TypeAnalyzer typeAnalyzer;

        PushDownDereferenceThroughUnnest(TypeAnalyzer typeAnalyzer)
        {
            this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project().with(source().matching(unnest().capturedAs(targetCapture)));
        }

        @Override
        public Result apply(ProjectNode node, Captures captures, Context context)
        {
            UnnestNode unnestNode = captures.get(targetCapture);
            Map<DereferenceExpression, Symbol> pushdownDereferences = getPushdownDereferences(context, node, captures.get(targetCapture), typeAnalyzer);

            if (pushdownDereferences.isEmpty()) {
                return Result.empty();
            }

            // Create new Project contains all pushdown symbols above original source
            Assignments assignments = Assignments.builder().putIdentities(unnestNode.getSource().getOutputSymbols()).putAll(HashBiMap.create(pushdownDereferences).inverse()).build();
            ProjectNode source = new ProjectNode(context.getIdAllocator().getNextId(), unnestNode.getSource(), assignments);

            // Create new UnnestNode
            UnnestNode newUnnest = new UnnestNode(context.getIdAllocator().getNextId(),
                    source,
                    ImmutableList.<Symbol>builder().addAll(unnestNode.getReplicateSymbols()).addAll(pushdownDereferences.values()).build(),
                    unnestNode.getUnnestSymbols(),
                    unnestNode.getOrdinalitySymbol(),
                    unnestNode.getJoinType(),
                    unnestNode.getFilter());
            return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(),
                    newUnnest,
                    node.getAssignments().rewrite(new DereferenceReplacer(pushdownDereferences))));
        }
    }

    private static Map<DereferenceExpression, Symbol> getPushdownDereferences(Context context, ProjectNode parent, PlanNode child, TypeAnalyzer typeAnalyzer)
    {
        Map<DereferenceExpression, Symbol> allDereferencesInProject = getDereferenceSymbolMap(parent.getAssignments().getExpressions(), context, typeAnalyzer);
        Set<Symbol> childSourceSymbols = child.getSources().stream().map(PlanNode::getOutputSymbols).flatMap(Collection::stream).collect(toImmutableSet());

        return allDereferencesInProject.entrySet().stream()
                .filter(entry -> childSourceSymbols.contains(getBase(entry.getKey())))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static PlanNode createProjectBelow(PlanNode planNode, Assignments dereferences, PlanNodeIdAllocator idAllocator)
    {
        if (dereferences.isEmpty()) {
            return planNode;
        }
        return new ProjectNode(idAllocator.getNextId(), planNode, Assignments.builder().putIdentities(planNode.getOutputSymbols()).putAll(dereferences).build());
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<DereferenceExpression, Symbol> expressions;

        DereferenceReplacer(Map<DereferenceExpression, Symbol> expressions)
        {
            this.expressions = requireNonNull(expressions, "expressions is null");
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (expressions.containsKey(node)) {
                return expressions.get(node).toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static List<DereferenceExpression> extractDereferenceExpressions(Expression expression)
    {
        ImmutableList.Builder<DereferenceExpression> builder = ImmutableList.builder();
        new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<DereferenceExpression>>()
        {
            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<DereferenceExpression> context)
            {
                context.add(node);
                return null;
            }
        }.process(expression, builder);
        return builder.build();
    }

    private static Map<DereferenceExpression, Symbol> getDereferenceSymbolMap(Collection<Expression> expressions, Context context, TypeAnalyzer typeAnalyzer)
    {
        Set<DereferenceExpression> dereferences = expressions.stream()
                .flatMap(expression -> extractDereferenceExpressions(expression).stream())
                .filter(PushDownDereferences::validPushDown)
                .collect(toImmutableSet());

        // When nested child and parent dereferences both exist, Pushdown rule will be trigger one more time
        // and lead to runtime error. E.g. [msg.foo, msg.foo.bar] => [exp, exp.bar] (should stop here but
        // since there are still dereferences, pushdown rule will trigger again)
        if (dereferences.stream().anyMatch(exp -> baseExists(exp, dereferences))) {
            return ImmutableMap.of();
        }

        return dereferences.stream()
                .collect(toImmutableMap(Function.identity(), expression -> newSymbol(expression, context, typeAnalyzer)));
    }

    private static Symbol newSymbol(Expression expression, Context context, TypeAnalyzer typeAnalyzer)
    {
        Type type = typeAnalyzer.getType(context.getSession(), context.getSymbolAllocator().getTypes(), expression);
        verify(type != null);
        return context.getSymbolAllocator().newSymbol(expression, type);
    }

    private static boolean baseExists(DereferenceExpression expression, Set<DereferenceExpression> dereferences)
    {
        Expression base = expression.getBase();
        while (base instanceof DereferenceExpression) {
            if (dereferences.contains(base)) {
                return true;
            }
            base = ((DereferenceExpression) base).getBase();
        }
        return false;
    }

    private static boolean validPushDown(DereferenceExpression dereference)
    {
        Expression base = dereference.getBase();
        return (base instanceof SymbolReference) || (base instanceof DereferenceExpression);
    }

    private static Symbol getBase(DereferenceExpression expression)
    {
        return getOnlyElement(extractAll(expression));
    }
}
