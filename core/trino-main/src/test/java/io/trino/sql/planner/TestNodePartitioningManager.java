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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.client.NodeVersion;
import io.trino.node.InternalNode;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;

class TestNodePartitioningManager
{
    @Test
    void testArbitraryBucketToNode()
    {
        List<InternalNode> nodes = ImmutableList.of(
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false),
                new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false));

        List<InternalNode> selectedNodes = NodePartitioningManager.createArbitraryBucketToNode(0, nodes, 64);

        assertThat(selectedNodes.stream().collect(groupingBy(InternalNode::getNodeIdentifier, counting()))).isEqualTo(
                ImmutableMap.of(
                        "other1", 16L,
                        "other2", 16L,
                        "other3", 16L,
                        "other4", 16L));
    }
}
