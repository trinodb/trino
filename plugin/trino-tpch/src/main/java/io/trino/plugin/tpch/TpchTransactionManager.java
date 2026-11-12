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
package io.trino.plugin.tpch;

import com.google.inject.Inject;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class TpchTransactionManager
{
    private final NodeManager nodeManager;
    private final ConcurrentMap<ConnectorTransactionHandle, Supplier<List<Node>>> transactions = new ConcurrentHashMap<>();

    @Inject
    public TpchTransactionManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    public ConnectorTransactionHandle beginTransaction()
    {
        TpchTransactionHandle transaction = new TpchTransactionHandle();
        // Split generation and bucket mapping must use the same workers, even if cluster membership changes.
        transactions.put(transaction, memoize(() -> nodeManager.getRequiredWorkerNodes().stream()
                .sorted(comparing(node -> node.getHostAndPort().toString()))
                .collect(toImmutableList())));
        return transaction;
    }

    public List<Node> getWorkerNodes(ConnectorTransactionHandle transaction)
    {
        Supplier<List<Node>> nodes = transactions.get(transaction);
        checkArgument(nodes != null, "no such transaction: %s", transaction);
        return nodes.get();
    }

    public void commit(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }

    public void rollback(ConnectorTransactionHandle transaction)
    {
        checkArgument(transactions.remove(transaction) != null, "no such transaction: %s", transaction);
    }
}
