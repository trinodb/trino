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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.spi.cache.CacheManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.CacheDataPlanNode;
import io.trino.sql.planner.plan.ChooseAlternativeNode;
import io.trino.sql.planner.plan.LoadCachedDataPlanNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isCacheEnabled;
import static io.trino.cache.CommonSubqueriesExtractor.extractCommonSubqueries;
import static io.trino.sql.planner.iterative.Lookup.noLookup;
import static java.util.Objects.requireNonNull;

/**
 * Extracts common subqueries and substitutes each subquery with {@link ChooseAlternativeNode}
 * consisting of 3 alternatives:
 * * original subplan
 * * subplan that caches data with {@link CacheManager}
 * * subplan that reads data from {@link CacheManager}
 */
public class CacheCommonSubqueries
{
    public static final int ORIGINAL_PLAN_ALTERNATIVE = 0;
    public static final int STORE_PAGES_ALTERNATIVE = 1;
    public static final int LOAD_PAGES_ALTERNATIVE = 2;

    private final boolean cacheEnabled;
    private final CacheController cacheController;
    private final PlannerContext plannerContext;
    private final Session session;
    private final PlanNodeIdAllocator idAllocator;
    private final SymbolAllocator symbolAllocator;

    public CacheCommonSubqueries(
            CacheController cacheController,
            PlannerContext plannerContext,
            Session session,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator)
    {
        this.cacheController = requireNonNull(cacheController, "cacheController is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.session = requireNonNull(session, "session is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.cacheEnabled = isCacheEnabled(session);
    }

    public PlanNode cacheSubqueries(PlanNode node)
    {
        if (!cacheEnabled) {
            return node;
        }

        Map<PlanNode, CommonPlanAdaptation> adaptations = extractCommonSubqueries(
                cacheController,
                plannerContext,
                session,
                idAllocator,
                symbolAllocator,
                node);

        // add alternatives for each adaptation
        ImmutableMap.Builder<PlanNode, PlanNode> nodeMapping = ImmutableMap.builder();
        for (Map.Entry<PlanNode, CommonPlanAdaptation> entry : adaptations.entrySet()) {
            CommonPlanAdaptation adaptation = entry.getValue();

            PlanNode storePagesAlternative =
                    adaptation.adaptCommonSubplan(
                            new CacheDataPlanNode(
                                    idAllocator.getNextId(),
                                    adaptation.getCommonSubplan()),
                            idAllocator);

            PlanNode loadPagesAlternative =
                    adaptation.adaptCommonSubplan(
                            new LoadCachedDataPlanNode(
                                    idAllocator.getNextId(),
                                    adaptation.getCommonSubplanSignature(),
                                    adaptation.getCommonDynamicFilterDisjuncts(),
                                    adaptation.getCommonColumnHandles(),
                                    adaptation.getCommonSubplan().getOutputSymbols()),
                            idAllocator);

            PlanNode[] alternatives = new PlanNode[3];
            // use static indexes explicitly to make ensure code stays consistent with static indexes
            alternatives[ORIGINAL_PLAN_ALTERNATIVE] = entry.getKey();
            alternatives[STORE_PAGES_ALTERNATIVE] = storePagesAlternative;
            alternatives[LOAD_PAGES_ALTERNATIVE] = loadPagesAlternative;

            nodeMapping.put(entry.getKey(), new ChooseAlternativeNode(
                    idAllocator.getNextId(),
                    ImmutableList.copyOf(alternatives),
                    adaptation.getCommonSubplanFilteredTableScan()));
        }

        return SimplePlanRewriter.rewriteWith(new PlanReplacer(nodeMapping.buildOrThrow()), node);
    }

    public static boolean isCacheChooseAlternativeNode(PlanNode node)
    {
        return isCacheChooseAlternativeNode(node, noLookup());
    }

    public static boolean isCacheChooseAlternativeNode(PlanNode node, Lookup lookup)
    {
        if (!(node instanceof ChooseAlternativeNode chooseAlternativeNode)) {
            return false;
        }

        if (chooseAlternativeNode.getSources().size() != 3) {
            return false;
        }

        return PlanNodeSearcher.searchFrom(chooseAlternativeNode.getSources().get(LOAD_PAGES_ALTERNATIVE), lookup)
                .whereIsInstanceOfAny(LoadCachedDataPlanNode.class)
                .matches();
    }

    public static LoadCachedDataPlanNode getLoadCachedDataPlanNode(ChooseAlternativeNode node)
    {
        checkArgument(isCacheChooseAlternativeNode(node), "ChooseAlternativeNode should contain cache alternatives");
        return (LoadCachedDataPlanNode) PlanNodeSearcher.searchFrom(node.getSources().get(LOAD_PAGES_ALTERNATIVE))
                .whereIsInstanceOfAny(LoadCachedDataPlanNode.class)
                .findOnlyElement();
    }

    private static class PlanReplacer
            extends SimplePlanRewriter<Void>
    {
        private final Map<PlanNode, PlanNode> mapping;

        public PlanReplacer(Map<PlanNode, PlanNode> mapping)
        {
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            if (mapping.containsKey(node)) {
                return mapping.get(node);
            }

            return context.defaultRewrite(node, context.get());
        }
    }
}
