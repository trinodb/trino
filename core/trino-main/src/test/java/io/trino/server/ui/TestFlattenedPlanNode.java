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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.server.ui.FlattenedPlanNode.flatten;
import static org.assertj.core.api.Assertions.assertThat;

class TestFlattenedPlanNode
{
    @Test
    void testFlatten()
    {
        JsonRenderedNode plan = node(
                "1",
                "Output",
                ImmutableMap.of("columnNames", "[name]"),
                ImmutableList.of("name := name_1"),
                node(
                        "2",
                        "InnerJoin",
                        ImmutableMap.of("criteria", "(nationkey = nationkey_0)"),
                        ImmutableList.of("Distribution: PARTITIONED"),
                        node("3", "TableScan", ImmutableMap.of("table", "tpch:tiny:nation"), ImmutableList.of()),
                        node("4", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[1]"), ImmutableList.of())));

        assertThat(flatten(plan)).containsExactly(
                new FlattenedPlanNode("1", "Output", ImmutableMap.of("columnNames", "[name]"), ImmutableList.of("name := name_1"), ImmutableList.of("2")),
                new FlattenedPlanNode("2", "InnerJoin", ImmutableMap.of("criteria", "(nationkey = nationkey_0)"), ImmutableList.of("Distribution: PARTITIONED"), ImmutableList.of("3", "4")),
                new FlattenedPlanNode("3", "TableScan", ImmutableMap.of("table", "tpch:tiny:nation"), ImmutableList.of(), ImmutableList.of()),
                new FlattenedPlanNode("4", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[1]"), ImmutableList.of(), ImmutableList.of()));
    }

    @Test
    void testFlattenAdaptivePlan()
    {
        // the initial plan is rendered before the current one, and both use the same node identifiers
        JsonRenderedNode plan = node(
                "1",
                "AdaptivePlan",
                ImmutableMap.of(),
                ImmutableList.of(),
                node(
                        "2",
                        "InnerJoin",
                        ImmutableMap.of("distribution", "PARTITIONED"),
                        ImmutableList.of(),
                        node("3", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[1]"), ImmutableList.of()),
                        node("4", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[2]"), ImmutableList.of())),
                node(
                        "2",
                        "InnerJoin",
                        ImmutableMap.of("distribution", "REPLICATED"),
                        ImmutableList.of(),
                        node("4", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[2]"), ImmutableList.of()),
                        node("5", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[3]"), ImmutableList.of())));

        assertThat(flatten(plan)).containsExactly(
                new FlattenedPlanNode("1", "AdaptivePlan", ImmutableMap.of(), ImmutableList.of(), ImmutableList.of("2", "2")),
                new FlattenedPlanNode("2", "InnerJoin", ImmutableMap.of("distribution", "REPLICATED"), ImmutableList.of(), ImmutableList.of("4", "5")),
                new FlattenedPlanNode("3", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[1]"), ImmutableList.of(), ImmutableList.of()),
                new FlattenedPlanNode("4", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[2]"), ImmutableList.of(), ImmutableList.of()),
                new FlattenedPlanNode("5", "RemoteSource", ImmutableMap.of("sourceFragmentIds", "[3]"), ImmutableList.of(), ImmutableList.of()));
    }

    private static JsonRenderedNode node(String id, String name, Map<String, String> descriptor, List<String> details, JsonRenderedNode... children)
    {
        return new JsonRenderedNode(id, name, descriptor, ImmutableList.of(), details, ImmutableList.of(), ImmutableList.copyOf(children));
    }
}
