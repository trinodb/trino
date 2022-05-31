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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;

import static com.google.common.base.Verify.verifyNotNull;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;

public abstract class SimplePlanRewriter<C>
        extends PlanVisitor<PlanNode, SimplePlanRewriter.RewriteContext<C>>
{
    public static <C> PlanNode rewriteWith(SimplePlanRewriter<C> rewriter, PlanNode node)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, null));
    }

    public static <C> PlanNode rewriteWith(SimplePlanRewriter<C> rewriter, PlanNode node, C context)
    {
        return node.accept(rewriter, new RewriteContext<>(rewriter, context));
    }

    @Override
    protected PlanNode visitPlan(PlanNode node, RewriteContext<C> context)
    {
        return context.defaultRewrite(node, context.get());
    }

    public static class RewriteContext<C>
    {
        private final C userContext;
        private final SimplePlanRewriter<C> nodeRewriter;

        private RewriteContext(SimplePlanRewriter<C> nodeRewriter, C userContext)
        {
            this.nodeRewriter = nodeRewriter;
            this.userContext = userContext;
        }

        public C get()
        {
            return userContext;
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children
         */
        public PlanNode defaultRewrite(PlanNode node)
        {
            return defaultRewrite(node, null);
        }

        /**
         * Invoke the rewrite logic recursively on children of the given node and swap it
         * out with an identical copy with the rewritten children
         */
        public PlanNode defaultRewrite(PlanNode node, C context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builderWithExpectedSize(node.getSources().size());
            node.getSources().forEach(source -> children.add(rewrite(source, context)));
            return replaceChildren(node, children.build());
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node, C userContext)
        {
            PlanNode result = node.accept(nodeRewriter, new RewriteContext<>(nodeRewriter, userContext));
            return verifyNotNull(result, "nodeRewriter returned null for %s", node.getClass().getName());
        }

        /**
         * This method is meant for invoking the rewrite logic on children while processing a node
         */
        public PlanNode rewrite(PlanNode node)
        {
            return rewrite(node, null);
        }
    }
}
