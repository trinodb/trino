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
package io.prestosql.plugin.tpch;

import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import static io.prestosql.plugin.tpch.TpchConnectorFactory.FAIL_PLANNING_PROPERTY;

public class TpchFailableSplitManager
        extends TpchSplitManager
{
    public TpchFailableSplitManager(NodeManager nodeManager, int splitsPerNode)
    {
        super(nodeManager, splitsPerNode);
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle tableHandle, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        if (session.getProperty(FAIL_PLANNING_PROPERTY, Boolean.class)) {
            throw new RuntimeException("Test failure of planning");
        }

        return super.getSplits(transaction, session, tableHandle, splitSchedulingStrategy);
    }
}
