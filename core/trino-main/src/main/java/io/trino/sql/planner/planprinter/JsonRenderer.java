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
package io.trino.sql.planner.planprinter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class JsonRenderer
        implements Renderer<String>
{
    private static final JsonCodec<JsonRenderedNode> CODEC = JsonCodec.jsonCodec(JsonRenderedNode.class);

    @Override
    public String render(PlanRepresentation plan)
    {
        return CODEC.toJson(renderJson(plan, plan.getRoot(), false));
    }

    protected JsonRenderedNode renderJson(PlanRepresentation plan, NodeRepresentation node, boolean isAdaptivePlanInitialNode)
    {
        ImmutableList.Builder<JsonRenderedNode> children = ImmutableList.builder();
        // Add initial children first
        children.addAll(renderChildren(plan, node.getInitialChildren(), true));
        children.addAll(renderChildren(plan, node.getChildren(), isAdaptivePlanInitialNode));
        return new JsonRenderedNode(
                node.getId().toString(),
                node.getName(),
                node.getDescriptor(),
                node.getOutputs(),
                node.getDetails(),
                node.getEstimates(),
                children.build());
    }

    private List<JsonRenderedNode> renderChildren(PlanRepresentation plan, List<PlanNodeId> children, boolean isAdaptivePlanInitialNode)
    {
        return children.stream()
                .map(isAdaptivePlanInitialNode ? plan::getInitialNode : plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(n -> renderJson(plan, n, isAdaptivePlanInitialNode))
                .collect(toImmutableList());
    }

    public static class JsonRenderedNode
    {
        private final String id;
        private final String name;
        private final Map<String, String> descriptor;
        private final List<Symbol> outputs;
        private final List<String> details;
        private final List<PlanNodeStatsAndCostSummary> estimates;
        private final List<JsonRenderedNode> children;

        public JsonRenderedNode(
                String id,
                String name,
                Map<String, String> descriptor,
                List<Symbol> outputs,
                List<String> details,
                List<PlanNodeStatsAndCostSummary> estimates,
                List<JsonRenderedNode> children)
        {
            this.id = requireNonNull(id, "id is null");
            this.name = requireNonNull(name, "name is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.outputs = requireNonNull(outputs, "outputs is null");
            this.details = requireNonNull(details, "details is null");
            this.estimates = requireNonNull(estimates, "estimates is null");
            this.children = requireNonNull(children, "children is null");
        }

        @JsonProperty
        public String getId()
        {
            return id;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public Map<String, String> getDescriptor()
        {
            return descriptor;
        }

        @JsonProperty
        public List<Symbol> getOutputs()
        {
            return outputs;
        }

        @JsonProperty
        public List<String> getDetails()
        {
            return details;
        }

        @JsonProperty
        public List<PlanNodeStatsAndCostSummary> getEstimates()
        {
            return estimates;
        }

        @JsonProperty
        public List<JsonRenderedNode> getChildren()
        {
            return children;
        }
    }
}
