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
package io.trino.plugin.pulsar.mock;

import io.trino.plugin.pulsar.PulsarConnectorConfig;
import io.trino.plugin.pulsar.PulsarConnectorId;
import io.trino.plugin.pulsar.PulsarSplit;
import io.trino.plugin.pulsar.PulsarSplitManager;
import io.trino.plugin.pulsar.PulsarTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.Collection;

public class MockPulsarSplitManager
        extends PulsarSplitManager
{
    private Collection<PulsarSplit> splits;

    public void setSplits(Collection<PulsarSplit> splits)
    {
        this.splits = splits;
    }

    public MockPulsarSplitManager(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig,
                                  ManagedLedgerFactory managedLedgerFactory) throws Exception
    {
        super(connectorId, pulsarConnectorConfig, new MockPulsarConnectorManagedLedgerFactory(managedLedgerFactory));
    }

    @Override
    protected Collection<PulsarSplit> getSplitsNonPartitionedTopic(int numSplits, TopicName topicName,
                                                                   PulsarTableHandle tableHandle, SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain,
                                                                   OffloadPoliciesImpl offloadPolicies) throws Exception
    {
        Collection<PulsarSplit> result = super.getSplitsNonPartitionedTopic(numSplits, topicName, tableHandle,
                schemaInfo, tupleDomain, offloadPolicies);
        result.forEach(splits::add);
        return result;
    }

    @Override
    protected Collection<PulsarSplit> getSplitsPartitionedTopic(int numSplits, TopicName topicName, PulsarTableHandle tableHandle,
                                                                SchemaInfo schemaInfo, TupleDomain<ColumnHandle> tupleDomain,
                                                                OffloadPoliciesImpl offloadPolicies) throws Exception
    {
        Collection<PulsarSplit> result = super.getSplitsPartitionedTopic(numSplits, topicName, tableHandle,
                schemaInfo, tupleDomain, offloadPolicies);
        result.forEach(splits::add);
        return result;
    }
}
