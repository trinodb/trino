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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.execution.RemoteTask;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.connector.CatalogHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TestingNodeSelectorFactory
        implements NodeSelectorFactory
{
    private final InternalNode currentNode;
    private final Supplier<Map<InternalNode, List<CatalogHandle>>> nodesSupplier;

    public TestingNodeSelectorFactory(InternalNode currentNode, Supplier<Map<InternalNode, List<CatalogHandle>>> nodesSupplier)
    {
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.nodesSupplier = requireNonNull(nodesSupplier, "nodesSupplier is null");
    }

    @Override
    public NodeSelector createNodeSelector(Session session, Optional<CatalogHandle> catalogName)
    {
        return new TestingNodeSelector(currentNode, createNodesSupplierForCatalog(catalogName, nodesSupplier));
    }

    private static Supplier<List<InternalNode>> createNodesSupplierForCatalog(Optional<CatalogHandle> catalogNameOptional, Supplier<Map<InternalNode, List<CatalogHandle>>> nodesSupplier)
    {
        return () -> {
            Map<InternalNode, List<CatalogHandle>> allNodes = nodesSupplier.get();
            if (catalogNameOptional.isEmpty()) {
                return ImmutableList.copyOf(allNodes.keySet());
            }
            CatalogHandle catalogHandle = catalogNameOptional.get();
            return allNodes.entrySet().stream()
                    .filter(entry -> entry.getValue().contains(catalogHandle))
                    .map(Map.Entry::getKey)
                    .collect(toImmutableList());
        };
    }

    public static class TestingNodeSupplier
            implements Supplier<Map<InternalNode, List<CatalogHandle>>>
    {
        private final Map<InternalNode, List<CatalogHandle>> nodes = new ConcurrentHashMap<>();

        public static TestingNodeSupplier create()
        {
            return new TestingNodeSupplier();
        }

        public static TestingNodeSupplier create(Map<InternalNode, List<CatalogHandle>> nodes)
        {
            TestingNodeSupplier testingNodeSupplier = new TestingNodeSupplier();
            nodes.forEach(testingNodeSupplier::addNode);
            return testingNodeSupplier;
        }

        private TestingNodeSupplier() {}

        public void addNode(InternalNode node, List<CatalogHandle> catalogs)
        {
            nodes.put(node, catalogs);
        }

        public void removeNode(InternalNode node)
        {
            nodes.remove(node);
        }

        @Override
        public Map<InternalNode, List<CatalogHandle>> get()
        {
            return nodes;
        }
    }

    private static class TestingNodeSelector
            implements NodeSelector
    {
        private final InternalNode currentNode;
        private final Supplier<List<InternalNode>> nodesSupplier;

        private TestingNodeSelector(InternalNode currentNode, Supplier<List<InternalNode>> nodesSupplier)
        {
            this.currentNode = requireNonNull(currentNode, "currentNode is null");
            this.nodesSupplier = requireNonNull(nodesSupplier, "nodesSupplier is null");
        }

        @Override
        public void lockDownNodes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InternalNode> allNodes()
        {
            return ImmutableList.copyOf(nodesSupplier.get());
        }

        @Override
        public InternalNode selectCurrentNode()
        {
            return currentNode;
        }

        @Override
        public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
        {
            return allNodes().stream()
                    .filter(node -> !excludedNodes.contains(node))
                    .limit(limit)
                    .collect(toImmutableList());
        }

        @Override
        public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
        {
            throw new UnsupportedOperationException();
        }
    }
}
