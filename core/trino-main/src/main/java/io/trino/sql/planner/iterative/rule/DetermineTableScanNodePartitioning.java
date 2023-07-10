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
package io.trino.sql.planner.iterative.rule;

import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableProperties;
import io.trino.metadata.TableProperties.TablePartitioning;
import io.trino.spi.connector.ConnectorBucketNodeMap;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static io.trino.SystemSessionProperties.getTableScanNodePartitioningMinBucketToTaskRatio;
import static io.trino.SystemSessionProperties.isUseTableScanNodePartitioning;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DetermineTableScanNodePartitioning
        implements Rule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan()
            .matching(tableScan -> tableScan.getUseConnectorNodePartitioning().isEmpty());

    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final TaskCountEstimator taskCountEstimator;

    public DetermineTableScanNodePartitioning(Metadata metadata, NodePartitioningManager nodePartitioningManager, TaskCountEstimator taskCountEstimator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableScanNode node, Captures captures, Context context)
    {
        TableProperties properties = metadata.getTableProperties(context.getSession(), node.getTable());
        if (properties.getTablePartitioning().isEmpty()) {
            return Result.ofPlanNode(node.withUseConnectorNodePartitioning(false));
        }

        TablePartitioning partitioning = properties.getTablePartitioning().get();
        Optional<ConnectorBucketNodeMap> bucketNodeMap = nodePartitioningManager.getConnectorBucketNodeMap(context.getSession(), partitioning.getPartitioningHandle());
        if (bucketNodeMap.map(ConnectorBucketNodeMap::hasFixedMapping).orElse(false)) {
            // use connector table scan node partitioning when bucket to node assignments are fixed
            return Result.ofPlanNode(node.withUseConnectorNodePartitioning(true));
        }

        if (!isUseTableScanNodePartitioning(context.getSession())) {
            return Result.ofPlanNode(node.withUseConnectorNodePartitioning(false));
        }

        int numberOfBuckets = bucketNodeMap.map(ConnectorBucketNodeMap::getBucketCount)
                .orElseGet(() -> nodePartitioningManager.getNodeCount(context.getSession(), partitioning.getPartitioningHandle()));
        int numberOfTasks = max(taskCountEstimator.estimateSourceDistributedTaskCount(context.getSession()), 1);

        return Result.ofPlanNode(node
                .withUseConnectorNodePartitioning((double) numberOfBuckets / numberOfTasks >= getTableScanNodePartitioningMinBucketToTaskRatio(context.getSession())));
    }
}
