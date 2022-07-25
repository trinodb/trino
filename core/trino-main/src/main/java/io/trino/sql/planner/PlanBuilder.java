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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.NodeRef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import static io.trino.sql.planner.ScopeAware.scopeAwareKey;
import static java.util.Objects.requireNonNull;

class PlanBuilder
{
    private final TranslationMap translations;
    private final PlanNode root;

    public PlanBuilder(TranslationMap translations, PlanNode root)
    {
        requireNonNull(translations, "translations is null");
        requireNonNull(root, "root is null");

        this.translations = translations;
        this.root = root;
    }

    public static PlanBuilder newPlanBuilder(RelationPlan plan, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, Session session, PlannerContext plannerContext)
    {
        return newPlanBuilder(plan, analysis, lambdaArguments, ImmutableMap.of(), session, plannerContext);
    }

    public static PlanBuilder newPlanBuilder(RelationPlan plan, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaArguments, Map<ScopeAware<Expression>, Symbol> mappings, Session session, PlannerContext plannerContext)
    {
        return new PlanBuilder(
                new TranslationMap(plan.getOuterContext(), plan.getScope(), analysis, lambdaArguments, plan.getFieldMappings(), mappings, session, plannerContext),
                plan.getRoot());
    }

    public PlanBuilder withNewRoot(PlanNode root)
    {
        return new PlanBuilder(translations, root);
    }

    public PlanBuilder withScope(Scope scope, List<Symbol> fields)
    {
        return new PlanBuilder(translations.withScope(scope, fields), root);
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public boolean canTranslate(Expression expression)
    {
        return translations.canTranslate(expression);
    }

    public Symbol translate(Expression expression)
    {
        return Symbol.from(translations.rewrite(expression));
    }

    public Expression rewrite(Expression expression)
    {
        return translations.rewrite(expression);
    }

    public TranslationMap getTranslations()
    {
        return translations;
    }

    public Scope getScope()
    {
        return translations.getScope();
    }

    public PlanBuilder appendProjections(Iterable<Expression> expressions, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return appendProjections(expressions, symbolAllocator, idAllocator, TranslationMap::rewrite, TranslationMap::canTranslate);
    }

    public <T extends Expression> PlanBuilder appendProjections(
            Iterable<T> expressions,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            BiFunction<TranslationMap, T, Expression> rewriter,
            BiPredicate<TranslationMap, T> alreadyHasTranslation)
    {
        Assignments.Builder projections = Assignments.builder();

        // add an identity projection for underlying plan
        projections.putIdentities(root.getOutputSymbols());

        Map<ScopeAware<Expression>, Symbol> mappings = new HashMap<>();
        for (T expression : expressions) {
            // Skip any expressions that have already been translated and recorded in the translation map, or that are duplicated in the list of exp
            if (!mappings.containsKey(scopeAwareKey(expression, translations.getAnalysis(), translations.getScope())) && !alreadyHasTranslation.test(translations, expression)) {
                Symbol symbol = symbolAllocator.newSymbol(expression, translations.getAnalysis().getType(expression));
                projections.put(symbol, rewriter.apply(translations, expression));
                mappings.put(scopeAwareKey(expression, translations.getAnalysis(), translations.getScope()), symbol);
            }
        }

        return new PlanBuilder(
                getTranslations().withAdditionalMappings(mappings),
                new ProjectNode(idAllocator.getNextId(), root, projections.build()));
    }
}
