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
package io.trino.server.ui;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Plan node in the shape the Web UI draws the live plan from: nodes of a plan fragment
 * are listed next to each other and refer to their sources by identifier.
 */
public record FlattenedPlanNode(
        String id,
        String name,
        Map<String, String> descriptor,
        List<String> details,
        List<String> sources)
{
    public FlattenedPlanNode
    {
        requireNonNull(id, "id is null");
        requireNonNull(name, "name is null");
        descriptor = ImmutableMap.copyOf(requireNonNull(descriptor, "descriptor is null"));
        details = ImmutableList.copyOf(requireNonNull(details, "details is null"));
        sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
    }

    public static List<FlattenedPlanNode> flatten(JsonRenderedNode root)
    {
        // The initial and the current plan of an adaptive plan share node identifiers. The node visited last wins.
        Map<String, FlattenedPlanNode> nodes = new LinkedHashMap<>();
        flatten(root, nodes);
        return ImmutableList.copyOf(nodes.values());
    }

    private static void flatten(JsonRenderedNode node, Map<String, FlattenedPlanNode> nodes)
    {
        nodes.put(node.getId(), new FlattenedPlanNode(
                node.getId(),
                node.getName(),
                node.getDescriptor(),
                node.getDetails(),
                node.getChildren().stream()
                        .map(JsonRenderedNode::getId)
                        .collect(toImmutableList())));
        node.getChildren().forEach(child -> flatten(child, nodes));
    }
}
