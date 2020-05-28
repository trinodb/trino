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
package io.prestosql.plugin.hive.rubix;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.qubole.rubix.spi.ClusterType.PRESTOSQL_CLUSTER_MANAGER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

// TODO: remove after https://github.com/prestosql/presto/issues/3907
public class PrestoClusterManager
        extends ClusterManager
{
    private static final String NODES_LIST = "nodesList";

    @Nullable
    private static volatile NodeManager nodeManager;

    @Nullable
    private LoadingCache<String, List<String>> nodesCache;

    static void setNodeManager(NodeManager nodeManager)
    {
        PrestoClusterManager.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void initialize(Configuration conf)
    {
        super.initialize(conf);
        nodesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(getNodeRefreshTime(), SECONDS)
                .build(CacheLoader.from(this::getNodesInternal));
    }

    @Override
    public ClusterType getClusterType()
    {
        return PRESTOSQL_CLUSTER_MANAGER;
    }

    @Override
    public boolean isMaster()
    {
        requireNonNull(nodeManager, "nodeManager is null");
        return nodeManager.getCurrentNode().isCoordinator();
    }

    @Override
    public List<String> getNodes()
    {
        requireNonNull(nodesCache, "nodesCache is null");
        return nodesCache.getUnchecked(NODES_LIST);
    }

    /*
     * This returns list of worker nodes when there are worker nodes in the cluster
     * If it is a single node cluster, it will return localhost information
     */
    private List<String> getNodesInternal()
    {
        requireNonNull(nodeManager, "nodeManager is null");
        List<String> workers = nodeManager.getWorkerNodes().stream()
                .filter(node -> !node.isCoordinator())
                .map(Node::getHost)
                .sorted()
                .collect(toImmutableList());

        if (workers.isEmpty()) {
            // Empty result set => server up and only master node running, return localhost has the only node
            // Do not need to consider failed nodes list as 1node cluster and server is up since it replied to allNodesRequest
            return ImmutableList.of(nodeManager.getCurrentNode().getHost());
        }

        return workers;
    }

    @Override
    public Integer getNextRunningNodeIndex(int startIndex)
    {
        return startIndex;
    }

    @Override
    public Integer getPreviousRunningNodeIndex(int startIndex)
    {
        return startIndex;
    }
}
