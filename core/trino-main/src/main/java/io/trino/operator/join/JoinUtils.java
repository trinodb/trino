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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;

/**
 * This class must be public as it is accessed via join compiler reflection.
 */
public final class JoinUtils
{
    private JoinUtils() {}

    public static List<Page> channelsToPages(List<ObjectArrayList<Block>> channels)
    {
        if (channels.isEmpty()) {
            return ImmutableList.of();
        }

        int pagesCount = channels.get(0).size();
        ImmutableList.Builder<Page> pagesBuilder = ImmutableList.builderWithExpectedSize(pagesCount);
        for (int pageIndex = 0; pageIndex < pagesCount; ++pageIndex) {
            Block[] blocks = new Block[channels.size()];
            for (int channelIndex = 0; channelIndex < blocks.length; ++channelIndex) {
                blocks[channelIndex] = channels.get(channelIndex).get(pageIndex);
            }
            pagesBuilder.add(new Page(blocks));
        }
        return pagesBuilder.build();
    }

    public static OptionalInt getSingleBigintJoinChannel(List<Integer> joinChannels, List<Type> types)
    {
        if (joinChannels.size() == 1 && types.get(getOnlyElement(joinChannels)) == BIGINT) {
            return OptionalInt.of(getOnlyElement(joinChannels));
        }
        return OptionalInt.empty();
    }

    public static boolean isBuildSideReplicated(PlanNode node)
    {
        checkArgument(node instanceof JoinNode || node instanceof SemiJoinNode);
        if (node instanceof JoinNode) {
            return PlanNodeSearcher.searchFrom(((JoinNode) node).getRight())
                    .recurseOnlyWhen(planNode -> planNode instanceof ProjectNode ||
                                    isLocalRepartitionExchange(planNode) ||
                                    isLocalGatherExchange(planNode))  // used in cross join case
                    .where(planNode -> isRemoteReplicatedExchange(planNode) || isRemoteReplicatedSourceNode(planNode))
                    .matches();
        }
        return PlanNodeSearcher.searchFrom(((SemiJoinNode) node).getFilteringSource())
                .recurseOnlyWhen(planNode -> planNode instanceof ProjectNode ||
                                isLocalGatherExchange(planNode))
                .where(joinNode -> isRemoteReplicatedExchange(joinNode) || isRemoteReplicatedSourceNode(joinNode))
                .matches();
    }

    public static Map<DynamicFilterId, Symbol> getJoinDynamicFilters(JoinNode joinNode)
    {
        List<DynamicFilterSourceNode> dynamicFilterSourceNodes = PlanNodeSearcher.searchFrom(joinNode.getRight())
                .where(DynamicFilterSourceNode.class::isInstance)
                .recurseOnlyWhen(node -> node instanceof ExchangeNode || node instanceof ProjectNode)
                .findAll();
        Map<DynamicFilterId, Symbol> dynamicFilters = joinNode.getDynamicFilters();
        // This method maybe called before AddJoinDynamicFilterSource has rewritten join dynamic filters to DynamicFilterSourceNode
        if (dynamicFilterSourceNodes.isEmpty()) {
            return dynamicFilters;
        }
        verify(
                dynamicFilters.isEmpty(),
                "Dynamic filters %s present in a join with a DynamicFilterSourceNode on it's build side", dynamicFilters);
        verify(dynamicFilterSourceNodes.size() == 1, "Expected only 1 dynamic filter source node");
        return dynamicFilterSourceNodes.get(0).getDynamicFilters();
    }

    public static Optional<DynamicFilterId> getSemiJoinDynamicFilterId(SemiJoinNode semiJoinNode)
    {
        List<DynamicFilterSourceNode> dynamicFilterSourceNodes = PlanNodeSearcher.searchFrom(semiJoinNode.getFilteringSource())
                .where(DynamicFilterSourceNode.class::isInstance)
                .recurseOnlyWhen(node -> node instanceof ExchangeNode || node instanceof ProjectNode)
                .findAll();
        Optional<DynamicFilterId> dynamicFilterId = semiJoinNode.getDynamicFilterId();
        // This method maybe called before AddSemiJoinDynamicFilterSource has rewritten join dynamic filters to DynamicFilterSourceNode
        if (dynamicFilterSourceNodes.isEmpty()) {
            return dynamicFilterId;
        }
        verify(
                dynamicFilterId.isEmpty(),
                "Dynamic filter %s present in a semi join with a DynamicFilterSourceNode on it's filtering source side", dynamicFilterId);
        verify(dynamicFilterSourceNodes.size() == 1, "Expected only 1 dynamic filter source node");
        return Optional.of(getOnlyElement(dynamicFilterSourceNodes.get(0).getDynamicFilters().keySet()));
    }

    private static boolean isRemoteReplicatedExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode exchangeNode)) {
            return false;
        }

        return exchangeNode.getScope() == REMOTE && exchangeNode.getType() == REPLICATE;
    }

    private static boolean isRemoteReplicatedSourceNode(PlanNode node)
    {
        if (!(node instanceof RemoteSourceNode remoteSourceNode)) {
            return false;
        }

        return remoteSourceNode.getExchangeType() == REPLICATE;
    }

    private static boolean isLocalRepartitionExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode exchangeNode)) {
            return false;
        }

        return exchangeNode.getScope() == LOCAL && exchangeNode.getType() == REPARTITION;
    }

    private static boolean isLocalGatherExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode exchangeNode)) {
            return false;
        }

        return exchangeNode.getScope() == LOCAL && exchangeNode.getType() == GATHER;
    }
}
