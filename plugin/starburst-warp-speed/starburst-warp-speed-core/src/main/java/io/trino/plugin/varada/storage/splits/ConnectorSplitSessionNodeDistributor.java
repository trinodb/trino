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
package io.trino.plugin.varada.storage.splits;

import io.airlift.log.Logger;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * used for TESTING!!
 */
public class ConnectorSplitSessionNodeDistributor
        implements ConnectorSplitNodeDistributor
{
    public static final int RANDOM_HASHING = -1;
    private static final Logger logger = Logger.get(ConnectorSplitSessionNodeDistributor.class);
    private final NodeManager nodeManager;
    private final String key;

    public ConnectorSplitSessionNodeDistributor(NodeManager nodeManager, String key)
    {
        this.nodeManager = nodeManager;
        this.key = key;
    }

    /**
     * gets as input a string key and returns a consistent Node for the key
     */
    @Override
    public Node getNode(String ignore)
    {
        Optional<Node> nodeIdentifierOpt = nodeManager
                .getWorkerNodes()
                .stream()
                .filter(x -> x.getNodeIdentifier().equals(key))
                .findFirst();
        Node node;
        try {
            if (nodeIdentifierOpt.isPresent()) {
                node = nodeIdentifierOpt.get();
            }
            else {
                int nodeNumber = Integer.parseInt(key);
                List<Node> workerNodes = new ArrayList<>(nodeManager.getWorkerNodes());
                if (nodeNumber == RANDOM_HASHING) {
                    Random random = new Random();
                    int index = random.nextInt(workerNodes.size());
                    node = workerNodes.get(index);
                }
                else {
                    node = workerNodes.get(nodeNumber);
                }
            }
        }
        catch (Exception e) {
            logger.error("invalid  session key=%s, workerNodeSize=%s for ConnectorSplitSessionNodeDistributor", key, nodeManager.getWorkerNodes().size());
            throw e;
        }
        return node;
    }

    @Override
    public void updateNodeBucketsIfNeeded()
    {
    }
}
